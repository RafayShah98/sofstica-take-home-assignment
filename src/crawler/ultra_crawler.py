import asyncio
import time
import logging
from typing import List, Set
from datetime import datetime, timedelta
from .async_github_client import AsyncGitHubClient, Repository
from ..database.models import DatabaseManager

logger = logging.getLogger(__name__)

class UltraCrawler:
    def __init__(self, github_client: AsyncGitHubClient, db_manager: DatabaseManager):
        self.github_client = github_client
        self.db_manager = db_manager
        self.seen_repository_ids: Set[str] = set()
        self.start_time = time.time()

    async def crawl_100k(self, batch_size: int = 100):
        """Ultra-aggressive crawling to reach 100,000+ repositories"""
        print("🚀 ULTRA-AGGRESSIVE 100K CRAWLER")
        print("🎯 TARGET: 100,000+ repositories")
        print("⚡ STRATEGY: Massive parallel queries + smart combinations")
        print("="*80)
        
        # Load existing repositories
        self._load_existing_repository_ids()
        
        total_crawled = 0
        round_number = 1
        
        while total_crawled < 100000 and round_number <= 5:  # Max 5 rounds
            print(f"\n🔄 ROUND {round_number} - Current: {total_crawled:,}/100,000")
            
            round_crawled = await self._execute_aggressive_round(100000 - total_crawled, batch_size)
            total_crawled += round_crawled
            
            print(f"✅ Round {round_number}: {round_crawled:,} repos (Total: {total_crawled:,})")
            
            if round_crawled < 1000:  # If we're not getting many new repos, stop
                print("💤 Diminishing returns, stopping...")
                break
                
            round_number += 1
        
        await self._print_final_stats(total_crawled)
        return total_crawled

    async def _execute_aggressive_round(self, target_count: int, batch_size: int) -> int:
        """Execute one aggressive crawling round with multiple strategies"""
        strategies = [
            self._combo_star_language_queries,
            self._time_based_queries,
            self._popularity_queries,
            self._miscellaneous_queries,
        ]
        
        total_round_crawled = 0
        
        for strategy in strategies:
            if total_round_crawled >= target_count:
                break
                
            strategy_crawled = await strategy(target_count - total_round_crawled, batch_size)
            total_round_crawled += strategy_crawled
            
            if strategy_crawled > 0:
                print(f"  ✅ {strategy.__name__}: {strategy_crawled:,} repos")
            
            # Small delay between strategies
            await asyncio.sleep(1)
        
        return total_round_crawled

    async def _combo_star_language_queries(self, target_count: int, batch_size: int) -> int:
        """Combination of stars + languages for maximum coverage"""
        print("  🔥 COMBO: Stars + Languages")
        
        languages = ["JavaScript", "Python", "Java", "TypeScript", "Go", "Rust", "C++", "C#"]
        star_ranges = [
            (1000, 5000), (500, 999), (100, 499), (50, 99), 
            (10, 49), (5, 9), (1, 4), (0, 0)
        ]
        
        queries = []
        for lang in languages:
            for min_stars, max_stars in star_ranges:
                if max_stars == 0:
                    query = f"language:{lang} stars:0"
                else:
                    query = f"language:{lang} stars:{min_stars}..{max_stars}"
                queries.append(query)
        
        # Add some pure star queries
        for min_stars in [10000, 5000, 1000, 500, 100, 50, 10, 1]:
            queries.append(f"stars:>={min_stars}")
        
        return await self._execute_parallel_queries(queries[:100], target_count, batch_size)

    async def _time_based_queries(self, target_count: int, batch_size: int) -> int:
        """Time-based queries for different periods"""
        print("  🔥 COMBO: Time-based searches")
        
        current_year = datetime.now().year
        queries = []
        
        # Last 3 years by month
        for year in range(current_year, current_year - 3, -1):
            for month in range(1, 13):
                start_date = f"{year}-{month:02d}-01"
                if month == 12:
                    end_date = f"{year}-12-31"
                else:
                    end_date = f"{year}-{month+1:02d}-01"
                queries.append(f"created:{start_date}..{end_date} stars:>=1")
        
        # Recent updates
        for days_ago in [7, 30, 90, 180]:
            date_str = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
            queries.append(f"pushed:>={date_str} stars:>=1")
        
        return await self._execute_parallel_queries(queries[:80], target_count, batch_size)

    async def _popularity_queries(self, target_count: int, batch_size: int) -> int:
        """Popularity-based queries"""
        print("  🔥 COMBO: Popularity metrics")
        
        queries = [
            # Forks-based
            "forks:>=1000", "forks:500..999", "forks:100..499", "forks:50..99",
            "forks:10..49", "forks:1..9",
            
            # Size-based (large projects)
            "size:>=100000", "size:50000..99999", "size:10000..49999",
            
            # Recently popular
            "stars:>=100 pushed:>=2024-01-01",
            "forks:>=50 created:>=2024-01-01",
            
            # Topic-based
            "topic:machine-learning stars:>=10",
            "topic:web-development stars:>=10", 
            "topic:api stars:>=10",
            "topic:docker stars:>=10",
            "topic:kubernetes stars:>=10",
        ]
        
        return await self._execute_parallel_queries(queries, target_count, batch_size)

    async def _miscellaneous_queries(self, target_count: int, batch_size: int) -> int:
        """Miscellaneous queries to catch everything"""
        print("  🔥 COMBO: Miscellaneous searches")
        
        queries = [
            # License-based
            "license:mit stars:>=1",
            "license:apache-2.0 stars:>=1", 
            "license:gpl-3.0 stars:>=1",
            
            # Organization-based (popular orgs)
            "user:microsoft stars:>=1",
            "user:google stars:>=1",
            "user:facebook stars:>=1", 
            "user:apple stars:>=1",
            "user:github stars:>=1",
            
            # Description keywords
            "awesome stars:>=10",
            "framework stars:>=10",
            "library stars:>=10",
            "boilerplate stars:>=10",
        ]
        
        return await self._execute_parallel_queries(queries, target_count, batch_size)

    async def _execute_parallel_queries(self, queries: List[str], target_count: int, batch_size: int) -> int:
        """Execute queries in parallel with conservative limits"""
        if not queries:
            return 0
        
        # Use fewer queries to avoid rate limits
        safe_queries = queries[:20]  # Reduced from 100 to 20
        
        print(f"    🔍 Executing {len(safe_queries)} queries (conservative mode)...")
        
        # Execute all queries in parallel
        all_repositories = await self.github_client.search_repositories_parallel(safe_queries, batch_size)
        
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
        
        # Print quick stats
        elapsed = time.time() - self.start_time
        rate = crawled_count / elapsed if elapsed > 0 else 0
        print(f"    📊 {crawled_count:,} new repos | Total unique: {len(self.seen_repository_ids):,} | Rate: {rate:.1f}/sec")
        
        return crawled_count

    def _load_existing_repository_ids(self):
        """Load existing repository IDs"""
        try:
            cursor = self.db_manager.conn.cursor()
            cursor.execute("SELECT github_id FROM repositories")
            existing_ids = {row[0] for row in cursor.fetchall()}
            self.seen_repository_ids.update(existing_ids)
            print(f"📊 Loaded {len(existing_ids):,} existing repository IDs")
        except Exception as e:
            print(f"⚠️  Could not load existing IDs: {e}")

    async def _print_final_stats(self, total_crawled: int):
        """Print final statistics"""
        total_time = time.time() - self.start_time
        
        print("\n" + "="*80)
        print("🎉 ULTRA CRAWLING COMPLETED!")
        print("="*80)
        print(f"📈 Total repositories crawled: {total_crawled:,}")
        print(f"⏱️  Total time: {total_time/60:.1f} minutes")
        print(f"🚀 Average speed: {total_crawled/total_time:.1f} repos/sec")
        print(f"💾 Unique repositories in DB: {len(self.seen_repository_ids):,}")
        print("="*80)