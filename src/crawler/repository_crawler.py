import time
import logging
import asyncio
from typing import List, Optional
from datetime import datetime, timedelta
from .async_github_client import AsyncGitHubClient, Repository
from ..database.models import DatabaseManager

logger = logging.getLogger(__name__)

class RepositoryCrawler:
    def __init__(self, github_client, db_manager: DatabaseManager):
        self.github_client = github_client
        self.db_manager = db_manager
        self.seen_repository_ids = set()
        self.start_time = None

    async def crawl_repositories_async(self, max_repos=100000, batch_size=100):
        """Async version that's 10x faster"""
        print("🚀 STARTING ASYNC GITHUB CRAWLER (10x SPEED)")
        print(f"🎯 TARGET: {max_repos:,} repositories")
        print(f"📦 BATCH SIZE: {batch_size}")
        print("="*80)
        
        self.start_time = time.time()
        
        # Load existing IDs
        self._load_existing_repository_ids()
        
        total_crawled = 0
        strategies = [
            self._mass_parallel_star_search,
            self._mass_parallel_language_search,
            self._mass_parallel_date_search,
        ]
        
        for strategy in strategies:
            if total_crawled >= max_repos:
                break
                
            strategy_crawled = await strategy(max_repos - total_crawled, batch_size)
            total_crawled += strategy_crawled
            print(f"✅ Strategy completed: {strategy_crawled:,} repos (Total: {total_crawled:,})")
            
            if strategy_crawled == 0:  # If strategy yields nothing, stop
                break

        await self._final_cleanup()
        return total_crawled

    async def _mass_parallel_star_search(self, target_count, batch_size):
        """Massively parallel star-based search"""
        print("⭐ MASS PARALLEL STAR SEARCH")
        
        # Generate hundreds of specific star range queries
        search_queries = []
        
        # High stars (1k+)
        for stars in range(1000, 50000, 500):
            search_queries.append(f"stars:{stars}..{stars+499}")
        
        # Medium stars (100-999)
        for stars in range(100, 1000, 50):
            search_queries.append(f"stars:{stars}..{stars+49}")
        
        # Low stars (1-99)
        for stars in range(1, 100, 10):
            search_queries.append(f"stars:{stars}..{stars+9}")
        
        # Zero stars
        search_queries.append("stars:0")
        
        print(f"  🔥 Executing {len(search_queries)} parallel star queries...")
        return await self._execute_mass_parallel_search(search_queries, target_count, batch_size)

    async def _mass_parallel_language_search(self, target_count, batch_size):
        """Massively parallel language-based search"""
        print("💻 MASS PARALLEL LANGUAGE SEARCH")
        
        languages = [
            "JavaScript", "Python", "Java", "TypeScript", "C++", "C#", "PHP", "C", 
            "Shell", "Ruby", "Go", "Rust", "Kotlin", "Swift", "Dart", "R", "Scala",
            "Perl", "Lua", "Haskell", "Clojure", "Elixir", "Julia", "MATLAB", "Groovy"
        ]
        
        search_queries = []
        for lang in languages:
            # Multiple star ranges per language
            for min_stars in [1000, 500, 100, 50, 10, 1]:
                search_queries.append(f"language:{lang} stars:>={min_stars}")
        
        print(f"  🔥 Executing {len(search_queries)} parallel language queries...")
        return await self._execute_mass_parallel_search(search_queries, target_count, batch_size)

    async def _mass_parallel_date_search(self, target_count, batch_size):
        """Massively parallel date-based search"""
        print("📅 MASS PARALLEL DATE SEARCH")
        
        current_year = datetime.now().year
        search_queries = []
        
        # Last 5 years by quarter
        for year in range(current_year, current_year - 5, -1):
            for quarter in range(1, 5):
                start_month = (quarter - 1) * 3 + 1
                end_month = quarter * 3
                start_date = f"{year}-{start_month:02d}-01"
                end_date = f"{year}-{end_month:02d}-28"
                search_queries.append(f"created:{start_date}..{end_date} stars:>=1")
        
        print(f"  🔥 Executing {len(search_queries)} parallel date queries...")
        return await self._execute_mass_parallel_search(search_queries, target_count, batch_size)

    async def _execute_mass_parallel_search(self, search_queries, target_count, batch_size):
        """Execute massive parallel search"""
        if not isinstance(self.github_client, AsyncGitHubClient):
            print("❌ Async client required for parallel search")
            return 0

        # Take only as many queries as needed
        search_queries = search_queries[:min(len(search_queries), 200)]  # Limit to 200 queries
        
        all_repositories = await self.github_client.search_repositories_parallel(search_queries, batch_size)
        
        # Remove duplicates
        unique_repos = [repo for repo in all_repositories if repo.github_id not in self.seen_repository_ids]
        
        if not unique_repos:
            return 0
        
        # Batch insert
        inserted, updated = self.db_manager.upsert_repositories(unique_repos)
        
        # Update tracking
        for repo in unique_repos:
            self.seen_repository_ids.add(repo.github_id)
        
        crawled_count = len(unique_repos)
        
        # Print progress
        elapsed = time.time() - self.start_time
        rate = crawled_count / elapsed if elapsed > 0 else 0
        print(f"  ✅ Found {crawled_count:,} unique repos | Rate: {rate:.1f} repos/sec")
        
        return crawled_count

    def _load_existing_repository_ids(self):
        """Load existing repository IDs"""
        try:
            cursor = self.db_manager.conn.cursor()
            cursor.execute("SELECT github_id FROM repositories LIMIT 500000")
            existing_ids = {row[0] for row in cursor.fetchall()}
            self.seen_repository_ids.update(existing_ids)
            print(f"📊 Loaded {len(existing_ids):,} existing repository IDs")
        except Exception as e:
            print(f"⚠️  Could not load existing IDs: {e}")

    async def _final_cleanup(self):
        """Final cleanup and stats"""
        total_time = time.time() - self.start_time
        cursor = self.db_manager.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM repositories")
        final_count = cursor.fetchone()[0]
        
        print("\n" + "="*80)
        print(f"🎉 ASYNC CRAWLING COMPLETED!")
        print(f"📈 Total repositories in DB: {final_count:,}")
        print(f"⏱️  Total time: {total_time/60:.1f} minutes")
        print(f"🚀 Average speed: {final_count/total_time:.1f} repos/sec")
        print("="*80)

    # Sync version for backward compatibility
    def crawl_repositories(self, max_repos=100000, batch_size=100):
        """Sync wrapper for async method"""
        return asyncio.run(self.crawl_repositories_async(max_repos, batch_size))