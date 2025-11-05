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
        """Sleep to maintain a buffer of requests and spread load."""
        if not rate_limit_info:
            time.sleep(1)  # Default sleep
            return

        remaining = rate_limit_info.get("remaining", 5000)
        
        # Simple sleep logic based on remaining rate limit
        if remaining < 100:
            print("⚠️  Low rate limit, sleeping 30 seconds...")
            time.sleep(30)
        elif remaining < 500:
            print("⏸️  Medium rate limit, sleeping 5 seconds...")
            time.sleep(5)
        else:
            time.sleep(1)  # Normal delay

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

        # Multiple search strategies to get around API limitations
        search_strategies = [
            self._crawl_by_stars,
            self._crawl_by_language,
            self._crawl_by_recently_updated,
        ]

        for strategy in search_strategies:
            if total_crawled >= max_repos:
                break
                
            try:
                print(f"\n🎯 EXECUTING STRATEGY: {strategy.__name__}")
                strategy_crawled = strategy(max_repos - total_crawled, batch_size)
                total_crawled += strategy_crawled
                consecutive_errors = 0
                print(f"✅ Strategy completed: {strategy_crawled:,} repos (Total: {total_crawled:,})")
                
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

    def _crawl_by_stars(self, target_count, batch_size):
        """Crawl repositories by different star ranges"""
        print("⭐ CRAWLING BY STAR RANGES")
        
        star_ranges = [
            (10000, None),  # 10k+ stars
            (5000, 9999),   # 5k-9,999 stars
            (1000, 4999),   # 1k-4,999 stars
            (500, 999),     # 500-999 stars
            (100, 499),     # 100-499 stars
            (50, 99),       # 50-99 stars
            (10, 49),       # 10-49 stars
            (1, 9),         # 1-9 stars
        ]
        
        total_crawled = 0
        
        for min_stars, max_stars in star_ranges:
            if total_crawled >= target_count:
                break
                
            if max_stars:
                star_query = f"stars:{min_stars}..{max_stars}"
            else:
                star_query = f"stars:>={min_stars}"
                
            print(f"  🔍 Searching: {star_query}")
            
            cursor = None
            has_more_pages = True
            page_number = 1
            
            while has_more_pages and total_crawled < target_count:
                try:
                    print(f"    📄 Page {page_number}...")
                    
                    repositories, cursor, rate_limit_info = self.github_client.search_repositories(
                        query=star_query, 
                        cursor=cursor, 
                        batch_size=batch_size
                    )

                    if not repositories:
                        print(f"    ✅ No more repositories for {star_query}")
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
                            print(f"    ✅ All repositories for {star_query} are processed")
                            has_more_pages = False
                            break

                    # Save to database
                    self.db_manager.upsert_repositories(unique_repos)
                    
                    # Update tracking
                    for repo in unique_repos:
                        self.seen_repository_ids.add(repo.github_id)

                    batch_crawled = len(unique_repos)
                    total_crawled += batch_crawled

                    # Print progress
                    self.print_status(total_crawled, target_count, f"Stars {star_query}", 
                                    batch_size, rate_limit_info.get("remaining", "Unknown") if rate_limit_info else "Unknown")
                    
                    self.print_progress_bar(total_crawled, target_count, 
                                          prefix=f'Stars {min_stars}+', 
                                          suffix=f'Page {page_number}')

                    # Rate limit handling
                    self.adaptive_sleep(rate_limit_info)

                    if not cursor:
                        print(f"    ✅ Completed all pages for {star_query}")
                        has_more_pages = False
                    else:
                        page_number += 1

                except Exception as e:
                    print(f"    ❌ Error: {e}")
                    break

        return total_crawled

    def _crawl_by_language(self, target_count, batch_size):
        """Crawl repositories by programming language"""
        print("💻 CRAWLING BY PROGRAMMING LANGUAGES")
        
        languages = [
            "JavaScript", "Python", "Java", "TypeScript", "C++", 
            "C#", "PHP", "C", "Shell", "Ruby", "Go", "Rust"
        ]
        
        total_crawled = 0
        
        for language in languages:
            if total_crawled >= target_count:
                break
                
            print(f"  🔍 Searching language: {language}")
            
            cursor = None
            has_more_pages = True
            page_number = 1
            
            while has_more_pages and total_crawled < target_count:
                try:
                    print(f"    📄 Page {page_number}...")
                    
                    repositories, cursor, rate_limit_info = self.github_client.search_repositories(
                        query=f"language:{language} stars:>1", 
                        cursor=cursor, 
                        batch_size=batch_size
                    )

                    if not repositories:
                        print(f"    ✅ No more {language} repositories")
                        has_more_pages = False
                        break

                    # Filter out duplicates
                    unique_repos = [repo for repo in repositories if repo.github_id not in self.seen_repository_ids]

                    if not unique_repos:
                        if cursor:
                            print(f"    🔄 No new {language} repos this page, continuing...")
                            page_number += 1
                            self.adaptive_sleep(rate_limit_info)
                            continue
                        else:
                            print(f"    ✅ All {language} repositories are processed")
                            has_more_pages = False
                            break

                    # Save to database
                    self.db_manager.upsert_repositories(unique_repos)
                    
                    # Update tracking
                    for repo in unique_repos:
                        self.seen_repository_ids.add(repo.github_id)

                    batch_crawled = len(unique_repos)
                    total_crawled += batch_crawled

                    # Print progress
                    self.print_status(total_crawled, target_count, f"Language {language}", 
                                    batch_size, rate_limit_info.get("remaining", "Unknown") if rate_limit_info else "Unknown")
                    
                    self.print_progress_bar(total_crawled, target_count, 
                                          prefix=f'Language {language}', 
                                          suffix=f'Page {page_number}')

                    # Rate limit handling
                    self.adaptive_sleep(rate_limit_info)

                    if not cursor:
                        print(f"    ✅ Completed all pages for {language}")
                        has_more_pages = False
                    else:
                        page_number += 1

                except Exception as e:
                    print(f"    ❌ Error with {language}: {e}")
                    break

        return total_crawled

    def _crawl_by_recently_updated(self, target_count, batch_size):
        """Crawl recently updated repositories"""
        print("🕒 CRAWLING RECENTLY UPDATED REPOSITORIES")
        
        total_crawled = 0
        
        cursor = None
        has_more_pages = True
        page_number = 1
        
        while has_more_pages and total_crawled < target_count:
            try:
                print(f"  📄 Page {page_number}...")
                
                repositories, cursor, rate_limit_info = self.github_client.search_repositories(
                    query="stars:>1 sort:updated-desc", 
                    cursor=cursor, 
                    batch_size=batch_size
                )

                if not repositories:
                    print("  ✅ No more recently updated repositories")
                    has_more_pages = False
                    break

                # Filter out duplicates
                unique_repos = [repo for repo in repositories if repo.github_id not in self.seen_repository_ids]

                if not unique_repos:
                    if cursor:
                        print("  🔄 No new repos this page, continuing...")
                        page_number += 1
                        self.adaptive_sleep(rate_limit_info)
                        continue
                    else:
                        print("  ✅ All recently updated repositories are processed")
                        has_more_pages = False
                        break

                # Save to database
                self.db_manager.upsert_repositories(unique_repos)
                
                # Update tracking
                for repo in unique_repos:
                    self.seen_repository_ids.add(repo.github_id)

                batch_crawled = len(unique_repos)
                total_crawled += batch_crawled

                # Print progress
                self.print_status(total_crawled, target_count, "Recently Updated", 
                                batch_size, rate_limit_info.get("remaining", "Unknown") if rate_limit_info else "Unknown")
                
                self.print_progress_bar(total_crawled, target_count, 
                                      prefix='Recently Updated', 
                                      suffix=f'Page {page_number}')

                # Rate limit handling
                self.adaptive_sleep(rate_limit_info)

                if not cursor:
                    print("  ✅ Completed all pages for recently updated")
                    has_more_pages = False
                else:
                    page_number += 1

            except Exception as e:
                print(f"  ❌ Error: {e}")
                break

        return total_crawled