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
        self.seen_repository_ids = set()
        
    def crawl_repositories(self, target_count: int = 100000, batch_size: int = 100) -> int:
        total_crawled = 0
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        logger.info(f"Starting to crawl {target_count} repositories using multiple search strategies")
        start_time = time.time()
        
        # Strategy 1: Crawl by different star ranges
        star_ranges = [
            (10000, 1000000),  # Very popular repos
            (1000, 9999),      # Popular repos  
            (100, 999),        # Medium popularity
            (10, 99),          # Less popular
            (1, 9),            # New/small repos
        ]
        
        # Strategy 2: Crawl by different languages
        popular_languages = [
            "JavaScript", "Python", "Java", "TypeScript", "C++", 
            "C#", "PHP", "C", "Shell", "Ruby", "Go", "Rust"
        ]
        
        # Start with star ranges
        for min_stars, max_stars in star_ranges:
            if total_crawled >= target_count:
                break
                
            logger.info(f"Crawling repositories with {min_stars}-{max_stars} stars...")
            cursor = None
            range_total = 0
            
            while total_crawled < target_count:
                try:
                    repositories, cursor = self.github_client.get_repositories_by_stars_range(
                        min_stars, max_stars, cursor, batch_size
                    )
                    
                    if not repositories:
                        logger.info(f"No more repositories in stars range {min_stars}-{max_stars}")
                        break
                    
                    # Filter out duplicates
                    unique_repos = [repo for repo in repositories if repo.github_id not in self.seen_repository_ids]
                    
                    if not unique_repos:
                        logger.info(f"All repositories in this batch are duplicates, moving to next range")
                        break
                    
                    # Upsert repositories
                    inserted, updated = self.db_manager.upsert_repositories(unique_repos)
                    
                    # Update tracking
                    for repo in unique_repos:
                        self.seen_repository_ids.add(repo.github_id)
                    
                    range_total += len(unique_repos)
                    total_crawled += len(unique_repos)
                    consecutive_errors = 0
                    
                    # Calculate progress
                    elapsed_time = time.time() - start_time
                    repos_per_second = total_crawled / elapsed_time if elapsed_time > 0 else 0
                    remaining_repos = target_count - total_crawled
                    eta_seconds = remaining_repos / repos_per_second if repos_per_second > 0 else 0
                    eta_hours = eta_seconds / 3600
                    
                    logger.info(f"Stars {min_stars}-{max_stars}: {len(unique_repos)} repos "
                              f"(Range: {range_total}, Total: {total_crawled}/{target_count}) | "
                              f"Rate: {repos_per_second:.1f} repos/sec | "
                              f"ETA: {eta_hours:.1f} hours")
                    
                    # Respect rate limits
                    if self.github_client.rate_limit_remaining < 500:
                        sleep_time = max(1, (self.github_client.rate_limit_reset - time.time()) + 10)
                        logger.warning(f"Approaching rate limit. Sleeping for {sleep_time:.1f} seconds")
                        time.sleep(sleep_time)
                    
                    # Small delay between requests
                    time.sleep(0.2)
                    
                    if not cursor:
                        logger.info(f"Completed stars range {min_stars}-{max_stars}")
                        break
                        
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Error crawling repositories (attempt {consecutive_errors}/{max_consecutive_errors}): {e}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("Too many consecutive errors, moving to next strategy")
                        break
                    
                    backoff_time = min(60 * consecutive_errors, 300)
                    logger.info(f"Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    continue
        
        # If we still need more repositories, use language-based search
        if total_crawled < target_count:
            logger.info(f"Switching to language-based search. Current count: {total_crawled}")
            
            for language in popular_languages:
                if total_crawled >= target_count:
                    break
                    
                logger.info(f"Crawling {language} repositories...")
                cursor = None
                language_total = 0
                
                while total_crawled < target_count:
                    try:
                        repositories, cursor = self.github_client.get_repositories_by_language(
                            language, cursor, batch_size
                        )
                        
                        if not repositories:
                            logger.info(f"No more {language} repositories")
                            break
                        
                        # Filter out duplicates
                        unique_repos = [repo for repo in repositories if repo.github_id not in self.seen_repository_ids]
                        
                        if not unique_repos:
                            logger.info(f"All {language} repositories in this batch are duplicates")
                            break
                        
                        # Upsert repositories
                        inserted, updated = self.db_manager.upsert_repositories(unique_repos)
                        
                        # Update tracking
                        for repo in unique_repos:
                            self.seen_repository_ids.add(repo.github_id)
                        
                        language_total += len(unique_repos)
                        total_crawled += len(unique_repos)
                        consecutive_errors = 0
                        
                        # Calculate progress
                        elapsed_time = time.time() - start_time
                        repos_per_second = total_crawled / elapsed_time if elapsed_time > 0 else 0
                        remaining_repos = target_count - total_crawled
                        eta_seconds = remaining_repos / repos_per_second if repos_per_second > 0 else 0
                        eta_hours = eta_seconds / 3600
                        
                        logger.info(f"Language {language}: {len(unique_repos)} repos "
                                  f"(Language: {language_total}, Total: {total_crawled}/{target_count}) | "
                                  f"Rate: {repos_per_second:.1f} repos/sec | "
                                  f"ETA: {eta_hours:.1f} hours")
                        
                        # Respect rate limits
                        if self.github_client.rate_limit_remaining < 500:
                            sleep_time = max(1, (self.github_client.rate_limit_reset - time.time()) + 10)
                            logger.warning(f"Approaching rate limit. Sleeping for {sleep_time:.1f} seconds")
                            time.sleep(sleep_time)
                        
                        # Small delay between requests
                        time.sleep(0.2)
                        
                        if not cursor:
                            logger.info(f"Completed language {language}")
                            break
                            
                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"Error crawling {language} repositories (attempt {consecutive_errors}/{max_consecutive_errors}): {e}")
                        
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error("Too many consecutive errors, moving to next language")
                            break
                        
                        backoff_time = min(60 * consecutive_errors, 300)
                        logger.info(f"Retrying in {backoff_time} seconds...")
                        time.sleep(backoff_time)
                        continue
        
        total_time = time.time() - start_time
        logger.info(f"Completed crawling {total_crawled} repositories in {total_time/3600:.2f} hours")
        return total_crawled