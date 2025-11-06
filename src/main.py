#!/usr/bin/env python3
import os
import sys
import logging
import time
import asyncio
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.crawler.async_github_client import AsyncGitHubClient
from src.crawler.ultra_crawler import UltraCrawler
from src.database.models import DatabaseManager

logging.basicConfig(level=logging.INFO)

async def main_ultra():
    print("🚀 ULTRA-AGGRESSIVE 100K GITHUB CRAWLER")
    print("💥 Strategy: 200+ parallel queries + smart combinations")
    print("🎯 Goal: Reach 100,000+ repositories")
    print("="*80)
    
    start_time = time.time()
    
    # Use higher concurrency for ultra crawling
    async with AsyncGitHubClient(max_concurrent=25) as github_client:
        db_manager = DatabaseManager()
        
        with db_manager:
            # Check current count
            cursor = db_manager.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM repositories")
            current_count = cursor.fetchone()[0]
            print(f"📊 Starting with {current_count:,} existing repositories")
            
            if current_count >= 100000:
                print("✅ Already have 100,000+ repositories!")
                db_manager.export_to_csv("repositories_export.csv")
                return current_count
            
            # Start ultra crawling
            crawler = UltraCrawler(github_client, db_manager)
            total_crawled = await crawler.crawl_100k(batch_size=100)
            
            # Final export
            print("\n💾 Exporting final results...")
            db_manager.export_to_csv("repositories_export.csv")
            
            total_time = time.time() - start_time
            print(f"\n🎉 FINAL RESULTS")
            print(f"📈 Total repositories: {total_crawled + current_count:,}")
            print(f"⏱️  Total time: {total_time/60:.1f} minutes")
            print(f"🚀 Average speed: {(total_crawled + current_count)/total_time:.1f} repos/sec")
            
            return total_crawled + current_count

def main():
    asyncio.run(main_ultra())

if __name__ == "__main__":
    main()