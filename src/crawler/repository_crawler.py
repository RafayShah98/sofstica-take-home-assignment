import time
import logging
from typing import List, Set
from .github_client import GitHubClient, Repository
from src.database.models import DatabaseManager
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

class RepositoryCrawler:
    def __init__(self, github_client: GitHubClient, db_manager: DatabaseManager):
        self.github_client = github_client
        self.db_manager = db_manager
        
    def crawl_repositories(self, target_count: int = 100000, batch_size: int = 100) -> int:
        total_crawled = 0
        start_time = time.time()
        
        logger.info(f"🚀 Starting optimized crawl for {target_count} repositories")
        
        # Generate diverse search queries to get around 1000-result limit
        search_queries = self._generate_search_queries()
        
        logger.info(f"Using {len(search_queries)} different search queries")
        
        # Get existing repository IDs to avoid duplicates
        existing_ids = self._get_existing_repository_ids()
        logger.info(f"Found {len(existing_ids)} existing repositories in database")
        
        # Fetch repositories in parallel
        all_repositories = self.github_client.get_repositories_parallel(search_queries, batch_size)
        
        # Remove duplicates
        unique_repos = []
        seen_ids = set(existing_ids)
        
        for repo in all_repositories:
            if repo.github_id not in seen_ids:
                unique_repos.append(repo)
                seen_ids.add(repo.github_id)
        
        logger.info(f"Retrieved {len(all_repositories)} total, {len(unique_repos)} unique new repositories")
        
        # Insert in batches for better performance
        if unique_repos:
            total_crawled = self._batch_insert_repositories(unique_repos, batch_size=1000)
        
        total_time = time.time() - start_time
        logger.info(f"✅ Completed crawling {total_crawled} repositories in {total_time/60:.1f} minutes")
        
        return total_crawled
    
    def _generate_search_queries(self) -> List[str]:
        """Generate diverse search queries to maximize repository coverage"""
        queries = []
        
        # Star-based queries (different ranges)
        star_ranges = [
            "stars:>=10000", "stars:5000..9999", "stars:1000..4999", 
            "stars:500..999", "stars:100..499", "stars:50..99",
            "stars:10..49", "stars:1..9"
        ]
        
        # Language-based queries (top languages)
        languages = [
            "JavaScript", "Python", "Java", "TypeScript", "C++", "C#", "PHP",
            "C", "Shell", "Ruby", "Go", "Rust", "Kotlin", "Swift", "Dart"
        ]
        
        # Date-based queries (recent activity)
        date_filters = [
            "pushed:>=2024-01-01", "pushed:>=2023-01-01", "created:>=2020-01-01"
        ]
        
        # Combine strategies
        for stars in star_ranges[:4]:  # Use top star ranges
            queries.append(f"{stars} sort:updated-desc")
            
        for language in languages[:8]:  # Use top languages
            queries.append(f"language:{language} stars:>10 sort:updated-desc")
            
        for date_filter in date_filters:
            queries.append(f"{date_filter} stars:>1 sort:updated-desc")
        
        # Some mixed queries
        queries.extend([
            "stars:>100 forks:>10 sort:forks-desc",
            "stars:>500 size:>1000 sort:stars-desc",
            "topic:machine-learning stars:>100",
            "topic:web-development stars:>50"
        ])
        
        return queries
    
    def _get_existing_repository_ids(self) -> Set[str]:
        """Get existing repository IDs from database to avoid duplicates"""
        try:
            with self.db_manager.conn.cursor() as cursor:
                cursor.execute("SELECT github_id FROM repositories LIMIT 50000")
                return {row[0] for row in cursor.fetchall()}
        except Exception as e:
            logger.warning(f"Could not fetch existing IDs: {e}")
            return set()
    
    def _batch_insert_repositories(self, repositories: List[Repository], batch_size: int = 1000) -> int:
        """Insert repositories in batches for better performance"""
        total_inserted = 0
        
        for i in range(0, len(repositories), batch_size):
            batch = repositories[i:i + batch_size]
            try:
                inserted, updated = self.db_manager.upsert_repositories(batch)
                total_inserted += inserted
                logger.info(f"📦 Batch {i//batch_size + 1}: Inserted {inserted}, Updated {updated} repositories")
                
                # Small delay to prevent database overload
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Failed to insert batch {i//batch_size + 1}: {e}")
                # Try smaller batch
                if batch_size > 100:
                    logger.info("Retrying with smaller batch size...")
                    for j in range(0, len(batch), 100):
                        small_batch = batch[j:j + 100]
                        try:
                            inserted, updated = self.db_manager.upsert_repositories(small_batch)
                            total_inserted += inserted
                        except Exception as e2:
                            logger.error(f"Failed to insert small batch: {e2}")
        
        return total_inserted