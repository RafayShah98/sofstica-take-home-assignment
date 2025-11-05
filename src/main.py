#!/usr/bin/env python3
"""
GitHub Repository Crawler Main Application
"""
import os
import sys
import logging

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

def main():
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize components
        github_client = GitHubClient()
        db_manager = DatabaseManager()
        
        with db_manager:
            # Setup database
            logger.info("Setting up database...")
            db_manager.setup_database()
            
            # Crawl 100,000 repositories as required
            crawler = RepositoryCrawler(github_client, db_manager)
            try:
                # Crawl repositories
                # Set larger target count for the assignment requirement
                total_crawled = crawler.crawl_repositories(max_repos=100000, batch_size=100)
                logger.info(f"Successfully crawled {total_crawled} repositories.")
            
                # Export data to CSV
                logger.info("Exporting data...")
                csv_data = db_manager.export_data('csv')
                
                # Write to file
                with open('repositories_export.csv', 'w', encoding='utf-8') as f:
                    f.write(csv_data)
                
                logger.info(f"Successfully crawled {total_crawled} repositories")
                logger.info("Data exported to repositories_export.csv")
                
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise

if __name__ == "__main__":
    main()