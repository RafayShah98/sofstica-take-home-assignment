import time
import logging
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from .github_client import GitHubClient
from ..database.models import DatabaseManager

logger = logging.getLogger(__name__)

class RepositoryCrawler:
    def __init__(self, github_client: GitHubClient, db_manager: DatabaseManager):
        self.github_client = github_client
        self.db_manager = db_manager
        self.seen_repository_ids = set()
        self.logger = logging.getLogger(__name__)
        self.start_time = None

    def print_progress_bar(self, current, total, prefix='', suffix='', length=50, fill='█'):
        """Print a progress bar to console"""
        percent = ("{0:.1f}").format(100 * (current / float(total)))
        filled_length = int(length * current // total)
        bar = fill * filled_length + '-' * (length - filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end='\r')
        if current == total:
            print()

    def print_status(self, current, total, date_str, batch_size, rate_limit):
        """Print detailed status to console"""
        elapsed = time.time() - self.start_time
        rate = current / elapsed if elapsed > 0 else 0
        eta_seconds = (total - current) / rate if rate > 0 else 0
        eta_hours = eta_seconds / 3600
        
        print("\n" + "="*80)
        print(f"📊 CRAWLING STATUS")
        print(f"🔄 Current Date: {date_str}")
        print(f"📈 Progress: {current:,} / {total:,} repositories ({current/total*100:.1f}%)")
        print(f"⏱️  Elapsed: {elapsed/3600:.1f}h | ETA: {eta_hours:.1f}h")
        print(f"🚀 Speed: {rate:.1f} repos/sec")
        print(f"📡 Rate Limit: {rate_limit} remaining")
        print(f"💾 Batch Size: {batch_size}")
        print("="*80)

    def adaptive_sleep(self, rate_limit_info):
        """Sleep to maintain a buffer of requests and spread load."""
        if not rate_limit_info:
            return

        remaining = rate_limit_info.get("remaining")
        reset_at_str = rate_limit_info.get("resetAt")
        cost = rate_limit_info.get("cost")

        if remaining is None or reset_at_str is None or cost is None:
            return

        reset_at = datetime.fromisoformat(reset_at_str.replace("Z", "+00:00"))
        buffer = 500  # Target a buffer of 500 points.

        if remaining < buffer:
            time_to_reset = (reset_at - datetime.now(timezone.utc)).total_seconds()
            print(f"⚠️  RATE LIMIT WARNING: Only {remaining} points left. Waiting {time_to_reset:.0f}s for reset...")
            if time_to_reset > 0:
                time.sleep(time_to_reset + 5)
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
            print(f"⚠️  RATE LIMIT CRITICAL: Not enough points. Waiting for reset...")
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
            print(f"⏸️  Throttling: {remaining} points left. Sleeping {sleep_duration:.1f}s...")
            time.sleep(sleep_duration)

    def crawl_repositories(self, max_repos=100000, batch_size=100):
        """
        Crawls GitHub repositories day by day, going backward in time,
        and stores them in the database.

        Args:
            max_repos (int): The maximum number of repositories to crawl.
            batch_size (int): The number of repositories to fetch per API request.
        """
        total_crawled = 0
        consecutive_errors = 0
        max_consecutive_errors = 10

        print("🚀 STARTING GITHUB REPOSITORY CRAWLER")
        print(f"🎯 TARGET: {max_repos:,} repositories")
        print(f"📦 BATCH SIZE: {batch_size}")
        print("="*80)
        
        self.start_time = time.time()

        # Start from today and go back in time
        current_date = datetime.utcnow()

        while total_crawled < max_repos:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"\n📅 CRAWLING DATE: {date_str}")
            print(f"📊 CURRENT TOTAL: {total_crawled:,} / {max_repos:,}")

            cursor = None
            date_total = 0
            has_more_pages = True
            page_number = 1

            while has_more_pages and total_crawled < max_repos:
                try:
                    print(f"\n📄 Fetching page {page_number} for {date_str}...")
                    
                    # The github client needs to return rate limit info
                    repositories, cursor, rate_limit_info = self.github_client.get_repositories_by_date(
                        date_str, cursor, batch_size
                    )

                    if not repositories:
                        print(f"❌ No repositories found for {date_str}")
                        has_more_pages = False
                        break

                    # Filter out duplicates
                    unique_repos = [repo for repo in repositories if repo.github_id not in self.seen_repository_ids]

                    if not unique_repos:
                        if cursor:  # If there's a next page, continue
                            print(f"🔄 No new repos this page, but more pages exist. Continuing...")
                            page_number += 1
                            self.adaptive_sleep(rate_limit_info)
                            continue
                        else: # No more pages for this date
                            print(f"✅ All repositories for {date_str} are already processed.")
                            has_more_pages = False
                            break

                    # Upsert repositories
                    print(f"💾 Saving {len(unique_repos)} new repositories to database...")
                    self.db_manager.upsert_repositories(unique_repos)

                    # Update tracking
                    for repo in unique_repos:
                        self.seen_repository_ids.add(repo.github_id)

                    date_total += len(unique_repos)
                    total_crawled += len(unique_repos)
                    consecutive_errors = 0

                    # Print detailed status
                    self.print_status(total_crawled, max_repos, date_str, batch_size, 
                                    rate_limit_info.get("remaining", "Unknown") if rate_limit_info else "Unknown")

                    # Print progress bar
                    self.print_progress_bar(total_crawled, max_repos, 
                                          prefix=f'Progress {date_str}', 
                                          suffix=f'Page {page_number} | {date_total} today')

                    # Adaptive delay to avoid hitting rate limits
                    self.adaptive_sleep(rate_limit_info)

                    if not cursor:
                        print(f"✅ Completed all pages for {date_str}")
                        has_more_pages = False
                    else:
                        page_number += 1

                except Exception as e:
                    consecutive_errors += 1
                    print(f"❌ ERROR on page {page_number} for {date_str}: {e}")
                    print(f"🔄 Attempt {consecutive_errors}/{max_consecutive_errors}")

                    if consecutive_errors >= max_consecutive_errors:
                        print("💥 TOO MANY ERRORS - STOPPING CRAWL")
                        return total_crawled

                    backoff_time = min(60 * consecutive_errors, 300)
                    print(f"⏸️  Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                    continue

            # Move to the previous day
            current_date -= timedelta(days=1)
            print(f"\n⏭️  Moving to previous day: {current_date.strftime('%Y-%m-%d')}")

            # Stop if we go too far back in time (e.g., before GitHub existed)
            if current_date.year < 2008:
                print("🛑 Reached the beginning of GitHub history (2008)")
                break

        total_time = time.time() - self.start_time
        print("\n" + "="*80)
        print(f"🎉 CRAWLING COMPLETED!")
        print(f"📈 Total repositories crawled: {total_crawled:,}")
        print(f"⏱️  Total time: {total_time/3600:.2f} hours")
        print(f"🚀 Average speed: {total_crawled/total_time:.1f} repos/sec")
        print("="*80)
        
        return total_crawled