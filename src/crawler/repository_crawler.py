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

    def print_status(self, current, total, strategy, batch_size, rate_limit):
        """Print detailed status to console"""
        elapsed = time.time() - self.start_time
        rate = current / elapsed if elapsed > 0 else 0
        eta_seconds = (total - current) / rate if rate > 0 else 0
        eta_hours = eta_seconds / 3600
        
        print("\n" + "="*80)
        print(f"📊 CRAWLING STATUS")
        print(f"🔄 Strategy: {strategy}")
        print(f"📈 Progress: {current:,} / {total:,} repositories ({current/total*100:.1f}%)")
        print(f"⏱️  Elapsed: {elapsed/3600:.1f}h | ETA: {eta_hours:.1f}h")
        print(f"🚀 Speed: {rate:.1f} repos/sec")
        print(f"📡 Rate Limit: {rate_limit} remaining")
        print(f"💾 Batch Size: {batch_size}")
        print("="*80)

    def adaptive_sleep(self, rate_limit_info):
        """Sleep based on rate limits"""
        if not rate_limit_info:
            time.sleep(2)  # Default sleep
            return

        remaining = rate_limit_info.get("remaining", 5000)
        
        if remaining < 50:
            print("⚠️  CRITICAL: Very low rate limit, sleeping 60 seconds...")
            time.sleep(60)
        elif remaining < 200:
            print("⚠️  WARNING: Low rate limit, sleeping 30 seconds...")
            time.sleep(30)
        elif remaining < 1000:
            time.sleep(5)  # Moderate delay
        else:
            time.sleep(2)  # Normal delay

    def crawl_repositories(self, max_repos=100000, batch_size=100):
        """
        Crawls GitHub repositories using multiple search strategies
        """
        total_crawled = 0
        consecutive_errors = 0
        max_consecutive_errors = 5

        print("🚀 STARTING GITHUB REPOSITORY CRAWLER")
        print(f"🎯 TARGET: {max_repos:,} repositories")
        print(f"📦 BATCH SIZE: {batch_size}")
        print("="*80)
        
        self.start_time = time.time()

        # Load existing repository IDs to avoid duplicates
        self._load_existing_repository_ids()
        print(f"📊 Found {len(self.seen_repository_ids):,} existing repositories in database")

        # Multiple search strategies to maximize results
        search_strategies = [
            self._crawl_by_star_ranges,
            self._crawl_by_languages,
            self._crawl_by_recently_updated,
            self._crawl_by_creation_date_ranges,
        ]

        for strategy in search_strategies:
            if total_crawled >= max_repos:
                break
                
            remaining = max_repos - total_crawled
            if remaining < 1000:  # If we're close to target, adjust strategy
                batch_size = min(50, batch_size)
                
            try:
                print(f"\n🎯 EXECUTING STRATEGY: {strategy.__name__}")
                strategy_crawled = strategy(remaining, batch_size)
                total_crawled += strategy_crawled
                consecutive_errors = 0
                print(f"✅ Strategy completed: {strategy_crawled:,} repos (Total: {total_crawled:,})")
                
                # If strategy didn't yield much, move to next one quickly
                if strategy_crawled < 1000 and remaining > 50000:
                    print("💤 Strategy yielded few results, moving to next...")
                
            except Exception as e:
                consecutive_errors += 1
                print(f"❌ Strategy failed: {e}")
                if consecutive_errors >= max_consecutive_errors:
                    print("💥 Too many consecutive errors, stopping crawl.")
                    break

        total_time = time.time() - self.start_time
        print("\n" + "="*80)
        print(f"🎉 CRAWLING COMPLETED!")
        print(f"📈 Total repositories crawled: {total_crawled:,}")
        print(f"⏱️  Total time: {total_time/3600:.2f} hours")
        print(f"🚀 Average speed: {total_crawled/total_time:.1f} repos/sec" if total_time > 0 else "N/A")
        print("="*80)
        
        return total_crawled

    def _load_existing_repository_ids(self):
        """Load existing repository IDs from database"""
        try:
            cursor = self.db_manager.conn.cursor()
            cursor.execute("SELECT github_id FROM repositories")
            existing_ids = {row[0] for row in cursor.fetchall()}
            self.seen_repository_ids.update(existing_ids)
            print(f"📊 Loaded {len(existing_ids):,} existing repository IDs")
        except Exception as e:
            print(f"⚠️  Could not load existing IDs: {e}")

    def _crawl_by_star_ranges(self, target_count, batch_size):
        """Crawl repositories by different star ranges - MOST EFFECTIVE STRATEGY"""
        print("⭐ CRAWLING BY STAR RANGES (Primary Strategy)")
        
        # More granular star ranges to get around 1000-result limit
        star_ranges = [
            (100000, None, "100k+ stars"),      # Very popular
            (50000, 99999, "50k-99k stars"),    
            (25000, 49999, "25k-49k stars"),
            (10000, 24999, "10k-24k stars"),
            (5000, 9999, "5k-9,999 stars"),   
            (2500, 4999, "2.5k-4,999 stars"),
            (1000, 2499, "1k-2,499 stars"),
            (500, 999, "500-999 stars"),     
            (250, 499, "250-499 stars"),     
            (100, 249, "100-249 stars"),     
            (50, 99, "50-99 stars"),       
            (25, 49, "25-49 stars"),       
            (10, 24, "10-24 stars"),       
            (5, 9, "5-9 stars"),         
            (1, 4, "1-4 stars"),         
            (0, 0, "0 stars"),           # Include zero-star repos
        ]
        
        total_crawled = 0
        
        for min_stars, max_stars, description in star_ranges:
            if total_crawled >= target_count:
                break
                
            if max_stars is None:
                star_query = f"stars:>={min_stars}"
            elif min_stars == max_stars == 0:
                star_query = "stars:0"
            else:
                star_query = f"stars:{min_stars}..{max_stars}"
                
            print(f"  🔍 Searching: {description}")
            
            total_crawled += self._execute_search_strategy(
                star_query, target_count - total_crawled, batch_size, f"Stars {description}"
            )

        return total_crawled

    def _crawl_by_languages(self, target_count, batch_size):
        """Crawl repositories by programming language"""
        print("💻 CRAWLING BY PROGRAMMING LANGUAGES")
        
        # Extended list of languages
        languages = [
            "JavaScript", "Python", "Java", "TypeScript", "C++", 
            "C#", "PHP", "C", "Shell", "Ruby", "Go", "Rust",
            "Kotlin", "Swift", "Dart", "R", "Scala", "Perl",
            "Lua", "Haskell", "Clojure", "Elixir", "Julia"
        ]
        
        total_crawled = 0
        
        for language in languages:
            if total_crawled >= target_count:
                break
                
            print(f"  🔍 Searching language: {language}")
            
            search_query = f"language:{language} stars:>=1"
            total_crawled += self._execute_search_strategy(
                search_query, target_count - total_crawled, batch_size, f"Language {language}"
            )

        return total_crawled

    def _crawl_by_recently_updated(self, target_count, batch_size):
        """Crawl recently updated repositories"""
        print("🕒 CRAWLING RECENTLY UPDATED REPOSITORIES")
        
        # Try different sort orders
        sort_orders = [
            "updated-desc",
            "stars-desc", 
            "forks-desc",
            "created-desc"
        ]
        
        total_crawled = 0
        
        for sort_order in sort_orders:
            if total_crawled >= target_count:
                break
                
            print(f"  🔍 Sorting by: {sort_order}")
            
            search_query = f"stars:>=1 sort:{sort_order}"
            total_crawled += self._execute_search_strategy(
                search_query, target_count - total_crawled, batch_size, f"Sorted {sort_order}"
            )

        return total_crawled

    def _crawl_by_creation_date_ranges(self, target_count, batch_size):
        """Crawl repositories by creation date ranges"""
        print("📅 CRAWLING BY CREATION DATE RANGES")
        
        # Create date ranges (yearly)
        current_year = datetime.now().year
        date_ranges = []
        
        for year in range(current_year, 2007, -1):  # From current year to 2008
            date_ranges.append((f"{year}-01-01", f"{year}-12-31", f"Created in {year}"))
        
        total_crawled = 0
        
        for start_date, end_date, description in date_ranges:
            if total_crawled >= target_count:
                break
                
            print(f"  🔍 Searching: {description}")
            
            search_query = f"created:{start_date}..{end_date} stars:>=1"
            total_crawled += self._execute_search_strategy(
                search_query, target_count - total_crawled, batch_size, description
            )

        return total_crawled

    def _execute_search_strategy(self, search_query, target_count, batch_size, strategy_name):
        """Execute a search strategy with pagination"""
        total_crawled = 0
        cursor = None
        has_more_pages = True
        page_number = 1
        max_pages_per_query = 10  # Limit pages per query to avoid hitting 1000 limit
        
        while has_more_pages and total_crawled < target_count and page_number <= max_pages_per_query:
            try:
                print(f"    📄 Page {page_number}...")
                
                repositories, cursor, rate_limit_info = self.github_client.search_repositories(
                    query=search_query, 
                    cursor=cursor, 
                    batch_size=batch_size
                )

                if not repositories:
                    print(f"    ✅ No more repositories for this query")
                    has_more_pages = False
                    break

                # Filter out duplicates
                unique_repos = [repo for repo in repositories if repo.github_id not in self.seen_repository_ids]

                if not unique_repos:
                    if cursor:
                        print(f"    🔄 No new repos this page, continuing...")
                        page_number += 1
                        self.adaptive_sleep(rate_limit_info)
                        continue
                    else:
                        print(f"    ✅ All repositories for this query are processed")
                        has_more_pages = False
                        break

                # Save to database
                inserted, updated = self.db_manager.upsert_repositories(unique_repos)
                
                # Update tracking
                for repo in unique_repos:
                    self.seen_repository_ids.add(repo.github_id)

                batch_crawled = len(unique_repos)
                total_crawled += batch_crawled

                # Print progress
                if page_number % 5 == 0 or batch_crawled > 0:  # Print every 5 pages or when we get results
                    self.print_status(total_crawled, target_count, strategy_name, 
                                    batch_size, rate_limit_info.get("remaining", "Unknown") if rate_limit_info else "Unknown")
                    
                    self.print_progress_bar(total_crawled, target_count, 
                                          prefix=f'{strategy_name[:20]:<20}', 
                                          suffix=f'Page {page_number}')

                # Rate limit handling
                self.adaptive_sleep(rate_limit_info)

                if not cursor:
                    print(f"    ✅ Completed all pages for this query")
                    has_more_pages = False
                else:
                    page_number += 1

            except Exception as e:
                print(f"    ❌ Error on page {page_number}: {e}")
                break

        return total_crawled