import time
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Optional
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

    # ================================================================
    # UI Helpers
    # ================================================================
    def print_progress_bar(self, current, total, prefix='', suffix='', length=50, fill='█'):
        percent = ("{0:.1f}").format(100 * (current / float(total)))
        filled_length = int(length * current // total)
        bar = fill * filled_length + '-' * (length - filled_length)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end='\r')
        if current >= total:
            print()

    def print_status(self, current, total, strategy, rate_limit):
        elapsed = time.time() - self.start_time
        rate = current / elapsed if elapsed > 0 else 0
        eta_seconds = (total - current) / rate if rate > 0 else 0
        eta_hours = eta_seconds / 3600
        print("\n" + "=" * 80)
        print(f"📊 CRAWLING STATUS - {strategy}")
        print(f"📈 {current:,}/{total:,} repos ({current/total*100:.1f}%)")
        print(f"⏱️ Elapsed: {elapsed/3600:.2f}h | ETA: {eta_hours:.2f}h")
        print(f"🚀 Speed: {rate:.1f} repos/sec | Rate Limit: {rate_limit}")
        print("=" * 80)

    # ================================================================
    # Rate Limit Handling
    # ================================================================
    def adaptive_sleep(self, rate_limit_info):
        if not rate_limit_info:
            time.sleep(2)
            return
        remaining = rate_limit_info.get("remaining", 5000)
        reset = rate_limit_info.get("reset", time.time() + 60)
        if remaining < 10:
            wait = max(5, reset - time.time() + 5)
            print(f"⚠️  Rate limit low ({remaining}), waiting {wait:.0f}s...")
            time.sleep(wait)
        elif remaining < 100:
            time.sleep(10)
        elif remaining < 500:
            time.sleep(5)
        else:
            time.sleep(2)

    # ================================================================
    # Main Crawl Controller
    # ================================================================
    def crawl_repositories(self, max_repos=100000, batch_size=100):
        total_crawled = 0
        self.start_time = time.time()

        print("=" * 80)
        print(f"🚀 STARTING GITHUB REPOSITORY CRAWLER")
        print(f"🎯 TARGET: {max_repos:,} repositories")
        print(f"📦 BATCH SIZE: {batch_size}")
        print("=" * 80)

        self._load_existing_repository_ids()
        print(f"📊 Loaded {len(self.seen_repository_ids):,} existing repository IDs")

        # Strategies
        strategies = [
            self._crawl_by_creation_date_and_stars,
            self._crawl_by_language_and_date
        ]

        for strategy in strategies:
            if total_crawled >= max_repos:
                break

            try:
                print(f"\n🎯 EXECUTING STRATEGY: {strategy.__name__}")
                crawled = strategy(max_repos - total_crawled, batch_size)
                total_crawled += crawled
                print(f"✅ {strategy.__name__} completed: {crawled:,} repos (Total: {total_crawled:,})")

            except Exception as e:
                print(f"❌ Strategy {strategy.__name__} failed: {e}")
                continue

        elapsed = time.time() - self.start_time
        print("\n" + "=" * 80)
        print(f"🎉 COMPLETED!")
        print(f"📈 Total: {total_crawled:,} repositories")
        print(f"⏱️ Duration: {elapsed/3600:.2f} hours")
        print(f"🚀 Avg Speed: {total_crawled/elapsed:.1f} repos/sec")
        print("=" * 80)

        return total_crawled

    # ================================================================
    # Strategies
    # ================================================================
    def _crawl_by_creation_date_and_stars(self, target_count, batch_size):
        """
        Crawl by month and star ranges to fully bypass GitHub's 1000 result cap.
        """
        print("📅 CRAWLING BY CREATION DATE + STAR RANGES")
        total_crawled = 0
        end_date = datetime.now().date()
        start_date = datetime(2008, 1, 1).date()

        star_ranges = [
            (0, 0), (1, 10), (11, 50), (51, 200),
            (201, 1000), (1001, 5000), (5001, 100000)
        ]

        current = start_date
        while current < end_date and total_crawled < target_count:
            next_month = current + relativedelta(months=1) - timedelta(days=1)
            for min_star, max_star in star_ranges:
                if total_crawled >= target_count:
                    break

                star_query = f"stars:{min_star}..{max_star}" if max_star > 0 else "stars:0"
                query = f"created:{current}..{next_month} {star_query}"
                label = f"{current} {min_star}-{max_star}⭐"

                print(f"  🔍 Query: {label}")
                crawled = self._execute_search_strategy(query, target_count - total_crawled, batch_size, label)
                total_crawled += crawled

            current += relativedelta(months=1)

        return total_crawled

    def _crawl_by_language_and_date(self, target_count, batch_size):
        """
        Secondary strategy: break by popular languages and quarterly date buckets.
        """
        print("💻 CRAWLING BY LANGUAGE + DATE RANGE")
        total_crawled = 0
        languages = [
            "Python", "JavaScript", "Java", "TypeScript", "Go", "Rust",
            "C++", "C#", "PHP", "Swift", "Kotlin", "Ruby"
        ]

        end_year = datetime.now().year
        date_ranges = []
        for year in range(end_year, 2007, -1):
            for quarter in range(1, 5):
                start_month = 3 * (quarter - 1) + 1
                end_month = start_month + 2
                start_date = datetime(year, start_month, 1).date()
                end_date = (datetime(year, end_month, 1) + relativedelta(months=1) - timedelta(days=1)).date()
                date_ranges.append((str(start_date), str(end_date)))

        for language in languages:
            for start, end in date_ranges:
                if total_crawled >= target_count:
                    break

                query = f"language:{language} created:{start}..{end} stars:>=1"
                label = f"{language} {start}..{end}"
                print(f"  🔍 Query: {label}")
                crawled = self._execute_search_strategy(query, target_count - total_crawled, batch_size, label)
                total_crawled += crawled

        return total_crawled

    # ================================================================
    # Database and Query Helpers
    # ================================================================
    def _load_existing_repository_ids(self):
        try:
            cursor = self.db_manager.conn.cursor()
            cursor.execute("SELECT github_id FROM repositories")
            ids = {r[0] for r in cursor.fetchall()}
            self.seen_repository_ids.update(ids)
        except Exception as e:
            print(f"⚠️ Failed to load existing IDs: {e}")

    def _execute_search_strategy(self, search_query, target_count, batch_size, label):
        total_crawled = 0
        cursor = None
        page = 1
        max_pages = 10

        while total_crawled < target_count and page <= max_pages:
            try:
                repos, cursor, rate_limit = self.github_client.search_repositories(
                    query=search_query, cursor=cursor, batch_size=batch_size
                )

                if not repos:
                    break

                # Filter new ones
                new_repos = [r for r in repos if r.github_id not in self.seen_repository_ids]
                if not new_repos:
                    page += 1
                    self.adaptive_sleep(rate_limit)
                    continue

                inserted, _ = self.db_manager.upsert_repositories(new_repos)
                total_crawled += inserted

                for r in new_repos:
                    self.seen_repository_ids.add(r.github_id)

                if page % 3 == 0:
                    remaining = rate_limit.get("remaining", "Unknown") if rate_limit else "?"
                    self.print_status(total_crawled, target_count, label, remaining)
                    self.print_progress_bar(total_crawled, target_count, prefix=label[:20])

                self.adaptive_sleep(rate_limit)

                if not cursor:
                    break
                page += 1

            except Exception as e:
                print(f"    ❌ Error on {label} p{page}: {e}")
                break

        return total_crawled
