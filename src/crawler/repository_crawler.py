import time
import logging
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from .github_client import GitHubClient, Repository
from src.database.models import DatabaseManager

logger = logging.getLogger(__name__)

class RepositoryCrawler:
    def __init__(self, github_client: GitHubClient, db_manager: DatabaseManager):
        self.github_client = github_client
        self.db_manager = db_manager
        self.seen_repository_ids = set()
        self.logger = logging.getLogger(__name__)
        self.start_time = None

    def adaptive_sleep(self, remaining, reset_at, cost):
        """Sleep to maintain a buffer of requests and spread load."""
        buffer = 500  # Target a buffer of 500 points.

        if remaining < buffer:
            time_to_reset = (reset_at - datetime.now(timezone.utc)).total_seconds()
            self.logger.warning(
                f"Rate limit remaining ({remaining}) is below buffer ({buffer}). "
                f"Waiting for reset in {time_to_reset:.0f}s."
            )
            if time_to_reset > 0:
                time.sleep(time_to_reset + 5)  # Wait for reset and a bit more
            return

        # If we have plenty of budget, don't sleep at all.
        if remaining > 4500:
            return

        # We have between `buffer` and 4500 points.
        # Let's calculate a sleep time that spreads the requests over the remaining time.
        time_to_reset = (reset_at - datetime.now(timezone.utc)).total_seconds()
        if time_to_reset <= 1:
            return  # No point in sleeping if window is about to reset

        # How many points can we use?
        points_to_use = remaining - buffer

        # How many requests can we make? (estimated)
        # Use max(cost, 1) to avoid division by zero if cost is 0 for some reason.
        num_requests_possible = points_to_use / max(cost, 1)

        if num_requests_possible <= 1:
            # Not enough points for even one more request, wait for reset.
            self.logger.warning(
                "Not enough rate limit points for next request. Waiting for reset."
            )
            if time_to_reset > 0:
                time.sleep(time_to_reset + 5)
            return

        # Spread these requests over the remaining time.
        # Time per request.
        time_per_request = time_to_reset / num_requests_possible

        # The loop itself takes time. Let's subtract an estimate for that.
        loop_overhead = 2.0
        sleep_duration = max(0, time_per_request - loop_overhead)

        # Cap the sleep to something reasonable, e.g. 15 seconds.
        sleep_duration = min(sleep_duration, 15)

        if sleep_duration > 0.1:  # Only log if sleep is meaningful
            self.logger.info(
                f"Throttling: {remaining} points left. "
                f"Sleeping for {sleep_duration:.2f}s to spread load."
            )
            time.sleep(sleep_duration)

    def crawl_repositories(self, max_repos=100000):
        """
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