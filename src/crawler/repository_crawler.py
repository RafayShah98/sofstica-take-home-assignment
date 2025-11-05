import time
import logging
from typing import List, Optional
from datetime import datetime, timedelta
from .github_client import GitHubClient, Repository
from src.database.models import DatabaseManager

logger = logging.getLogger(__name__)

class RepositoryCrawler:
    def __init__(self, github_client: GitHubClient, db_manager: DatabaseManager):
        self.github_client = github_client
        self.db_manager = db_manager
        self.seen_repository_ids = set()

    def adaptive_sleep(self):
        """Dynamically calculates sleep time to avoid rate limiting."""
        remaining = self.github_client.rate_limit_remaining
        cost = self.github_client.last_query_cost
        
        # If we have plenty of requests left, no need to sleep long
        if remaining > 1000:
            time.sleep(0.1) # A minimal polite sleep
            return

        # If we are getting low, calculate sleep time more carefully
        time_to_reset = self.github_client.rate_limit_reset - time.time()
        
        if time_to_reset <= 0:
            time.sleep(1) # Reset time has passed, sleep a bit just in case
            return
            
        # Calculate how many requests we can make per second, leaving a buffer
        safe_remaining = max(0, remaining - 100) 
        requests_per_sec = safe_remaining / time_to_reset
        
        # If our budget is less than 2 requests per second, we need to slow down
        if requests_per_sec < 2:
            # Calculate sleep time to spread requests evenly until the reset
            sleep_for = (1 / requests_per_sec) * cost
            # Cap sleep time to avoid excessively long waits between requests
            sleep_for = min(sleep_for, 15) 
            logger.info(f"Rate limit low ({remaining} left). Throttling requests. Sleeping for {sleep_for:.2f}s.")
            time.sleep(sleep_for)
        else:
            # We can afford to go faster
            time.sleep(0.25) # Sleep a bit to be polite
        
    def crawl_repositories(self, target_count: int = 100000, batch_size: int = 100) -> int:
        total_crawled = 0
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        logger.info(f"Starting to crawl {target_count} repositories using date-based search")
        start_time = time.time()
        
        # Start from today and go back in time
        current_date = datetime.utcnow()
        
        while total_crawled < target_count:
            date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"Crawling repositories created on {date_str}...")
            
            cursor = None
            date_total = 0
            
            while total_crawled < target_count:
                try:
                    repositories, cursor = self.github_client.get_repositories_by_date(
                        date_str, cursor, batch_size
                    )
                    
                    if not repositories:
                        logger.info(f"No more repositories for {date_str}")
                        break
                    
                    # Filter out duplicates
                    unique_repos = [repo for repo in repositories if repo.github_id not in self.seen_repository_ids]
                    
                    if not unique_repos:
                        if cursor:  # If there's a next page, continue
                            logger.info("No new repositories in this batch, but more pages exist. Continuing...")
                            continue
                        else: # No more pages for this date
                            logger.info(f"All repositories for {date_str} are duplicates or processed.")
                            break

                    # Upsert repositories
                    inserted, updated = self.db_manager.upsert_repositories(unique_repos)
                    
                    # Update tracking
                    for repo in unique_repos:
                        self.seen_repository_ids.add(repo.github_id)
                    
                    date_total += len(unique_repos)
                    total_crawled += len(unique_repos)
                    consecutive_errors = 0
                    
                    # Calculate progress
                    elapsed_time = time.time() - start_time
                    repos_per_second = total_crawled / elapsed_time if elapsed_time > 0 else 0
                    remaining_repos = target_count - total_crawled
                    eta_seconds = remaining_repos / repos_per_second if repos_per_second > 0 else 0
                    eta_hours = eta_seconds / 3600
                    
                    logger.info(f"Date {date_str}: Fetched {len(unique_repos)} new repos "
                              f"(Day Total: {date_total}, Grand Total: {total_crawled}/{target_count}) | "
                              f"Rate: {repos_per_second:.1f} repos/sec | "
                              f"ETA: {eta_hours:.1f} hours")
                    
                    # Adaptive delay to avoid hitting rate limits
                    self.adaptive_sleep()
                    
                    if not cursor:
                        logger.info(f"Completed crawling for date {date_str}")
                        break
                        
                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Error crawling repositories for date {date_str} (attempt {consecutive_errors}/{max_consecutive_errors}): {e}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error("Too many consecutive errors, stopping crawl.")
                        return total_crawled
                    
                    backoff_time = min(60 * consecutive_errors, 300)
                    logger.info(f"Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    continue
            
            # Move to the previous day
            current_date -= timedelta(days=1)
            
            # Stop if we go too far back in time (e.g., before GitHub existed)
            if current_date.year < 2008:
                logger.info("Reached the beginning of GitHub history.")
                break

        total_time = time.time() - start_time
        logger.info(f"Completed crawling {total_crawled} repositories in {total_time/3600:.2f} hours")
        return total_crawled