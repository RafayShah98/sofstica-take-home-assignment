import time
import logging
from typing import List, Optional
from .github_client import GitHubClient, Repository
from src.database.models import DatabaseManager

logger = logging.getLogger(__name__)

class RepositoryCrawler:
    def __init__(self, github_client: GitHubClient, db_manager: DatabaseManager):
        self.github_client = github_client
        self.db_manager = db_manager
        
    def crawl_repositories(self, target_count: int = 100000, batch_size: int = 100) -> int:
        total_crawled = 0
        cursor = None
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        logger.info(f"Starting to crawl {target_count} repositories")
        start_time = time.time()
        
        while total_crawled < target_count and consecutive_errors < max_consecutive_errors:
            try:
                repositories, cursor = self.github_client.get_repositories_batch(cursor, batch_size)
                
                if not repositories:
                    logger.info("No more repositories to crawl")
                    break
                
                # Upsert repositories
                inserted, updated = self.db_manager.upsert_repositories(repositories)
                total_crawled += len(repositories)
                consecutive_errors = 0  # Reset error counter on success
                
                # Calculate progress and ETA
                elapsed_time = time.time() - start_time
                repos_per_second = total_crawled / elapsed_time if elapsed_time > 0 else 0
                remaining_repos = target_count - total_crawled
                eta_seconds = remaining_repos / repos_per_second if repos_per_second > 0 else 0
                eta_hours = eta_seconds / 3600
                
                logger.info(f"Crawled {len(repositories)} repositories "
                          f"(Total: {total_crawled}/{target_count}, "
                          f"Inserted: {inserted}, Updated: {updated}) | "
                          f"Rate: {repos_per_second:.1f} repos/sec | "
                          f"ETA: {eta_hours:.1f} hours")
                
                # Respect rate limits - be more aggressive with larger batch size
                if self.github_client.rate_limit_remaining < 500:
                    sleep_time = max(1, (self.github_client.rate_limit_reset - time.time()) + 10)
                    logger.warning(f"Approaching rate limit. Sleeping for {sleep_time:.1f} seconds")
                    time.sleep(sleep_time)
                
                # Small delay between requests to avoid overwhelming the API
                time.sleep(0.2)
                
                if not cursor:
                    logger.info("No more pages to crawl")
                    break
                    
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error crawling repositories (attempt {consecutive_errors}/{max_consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("Too many consecutive errors, stopping crawl")
                    break
                    
                # Exponential backoff with cap
                backoff_time = min(60 * consecutive_errors, 300)  # Cap at 5 minutes
                logger.info(f"Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
                continue
        
        total_time = time.time() - start_time
        logger.info(f"Completed crawling {total_crawled} repositories in {total_time/3600:.2f} hours")
        return total_crawled