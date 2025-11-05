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
        
    def crawl_repositories(self, target_count: int = 100, batch_size: int = 50) -> int:
        total_crawled = 0
        cursor = None
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        logger.info(f"Starting to crawl {target_count} repositories")
        
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
                
                logger.info(f"Crawled {len(repositories)} repositories "
                          f"(Total: {total_crawled}, Inserted: {inserted}, Updated: {updated})")
                
                # Respect rate limits
                if self.github_client.rate_limit_remaining < 100:
                    sleep_time = max(1, (self.github_client.rate_limit_reset - time.time()) + 10)
                    logger.warning(f"Approaching rate limit. Sleeping for {sleep_time} seconds")
                    time.sleep(sleep_time)
                
                # Small delay between requests
                time.sleep(0.5)
                
                if not cursor:
                    logger.info("No more pages to crawl")
                    break
                    
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error crawling repositories (attempt {consecutive_errors}/{max_consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("Too many consecutive errors, stopping crawl")
                    break
                    
                time.sleep(60 * consecutive_errors)  # Exponential backoff
                continue
        
        logger.info(f"Completed crawling {total_crawled} repositories")
        return total_crawled
