#!/usr/bin/env python3
"""
GitHub Repository Crawler Main Application
"""
import os
import sys
import logging
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.crawler.github_client import GitHubClient
from src.crawler.repository_crawler import RepositoryCrawler
from src.database.models import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def print_banner():
    """Print a nice banner at startup"""
    print("\n" + "="*80)
    print("🚀 GITHUB REPOSITORY CRAWLER")
    print("="*80)
    print("📊 Target: 100,000 repositories")
    print("💾 Database: SQLite")
    print("🔑 GitHub API: Authenticated")
    print("="*80 + "\n")

def print_summary(total_crawled, start_time, csv_path):
    """Print summary after completion"""
    total_time = time.time() - start_time
    print("\n" + "="*80)
    print("📋 CRAWL SUMMARY")
    print("="*80)
    print(f"✅ Status: COMPLETED")
    print(f"📈 Repositories Crawled: {total_crawled:,}")
    print(f"⏱️  Total Time: {total_time/3600:.2f} hours")
    print(f"🚀 Average Speed: {total_crawled/total_time:.1f} repos/sec")
    print(f"💾 Data Exported: {csv_path}")
    print("="*80)

def main():
    logger = logging.getLogger(__name__)
    start_time = time.time()
    
    try:
        print_banner()
        
        # Initialize components
        print("🔄 Initializing components...")
        github_client = GitHubClient()
        db_manager = DatabaseManager()
        
        with db_manager:
            # Setup database
            print("💾 Setting up database...")
            db_manager.setup_database()
            print("✅ Database ready")
            
            # Crawl 100,000 repositories as required
            print("🎯 Starting repository crawler...")
            crawler = RepositoryCrawler(github_client, db_manager)
            
            try:
                # Crawl repositories
                print("🔄 Beginning crawl process...")
                total_crawled = crawler.crawl_repositories(max_repos=100000, batch_size=100)
                
                # Export data to CSV
                print("\n💾 Exporting data to CSV...")
                output_csv_path = "repositories_export.csv"
                db_manager.export_to_csv(output_csv_path)
                print(f"✅ Data exported to {output_csv_path}")
                
                # Print final summary
                print_summary(total_crawled, start_time, output_csv_path)
                
            except Exception as crawl_error:
                print(f"❌ CRAWLING FAILED: {crawl_error}")
                logger.error(f"Crawling failed: {crawl_error}")

    except Exception as e:
        print(f"💥 APPLICATION FAILED: {e}")
        logger.error(f"Application failed: {e}")

if __name__ == "__main__":
    main()