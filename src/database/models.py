import os
import logging
from typing import List, Tuple
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import csv
import json

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS repositories (
    id BIGSERIAL PRIMARY KEY,
    github_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    owner_login VARCHAR(255) NOT NULL,
    full_name VARCHAR(511) NOT NULL,
    description TEXT,
    stargazers_count INTEGER NOT NULL,
    forks_count INTEGER,
    open_issues_count INTEGER,
    language VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    pushed_at TIMESTAMP,
    size INTEGER,
    archived BOOLEAN,
    disabled BOOLEAN,
    license_info VARCHAR(255),
    crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_repositories_github_id ON repositories(github_id);
CREATE INDEX IF NOT EXISTS idx_repositories_stargazers_count ON repositories(stargazers_count);
CREATE INDEX IF NOT EXISTS idx_repositories_updated_at ON repositories(updated_at);
"""

class DatabaseManager:
    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string or os.getenv(
            'DATABASE_URL', 
            'postgresql://postgres:postgres@localhost:5432/github_crawler'
        )
        try:
            self.conn = psycopg2.connect(self.connection_string)
            logger.info("✅ Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
            raise
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def setup_database(self):
        """Create tables and indexes"""
        cursor = self.conn.cursor()
        
        # Execute each statement separately for PostgreSQL
        statements = [stmt.strip() for stmt in SCHEMA.split(';') if stmt.strip()]
        for statement in statements:
            cursor.execute(statement)
            
        self.conn.commit()
        logger.info("✅ PostgreSQL database schema created successfully")
    
    def upsert_repositories(self, repositories: List) -> Tuple[int, int]:
        """Upsert repositories and return counts of inserted and updated rows"""
        if not repositories:
            return 0, 0
            
        # Use bulk method for large batches for better performance
        if len(repositories) > 500:
            return self.bulk_upsert_repositories(repositories)
            
        insert_query = """
        INSERT INTO repositories (
            github_id, name, owner_login, full_name, description, stargazers_count,
            forks_count, open_issues_count, language, created_at, updated_at,
            pushed_at, size, archived, disabled, license_info
        ) VALUES %s
        ON CONFLICT (github_id) DO UPDATE SET
            name = EXCLUDED.name,
            owner_login = EXCLUDED.owner_login,
            full_name = EXCLUDED.full_name,
            description = EXCLUDED.description,
            stargazers_count = EXCLUDED.stargazers_count,
            forks_count = EXCLUDED.forks_count,
            open_issues_count = EXCLUDED.open_issues_count,
            language = EXCLUDED.language,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            pushed_at = EXCLUDED.pushed_at,
            size = EXCLUDED.size,
            archived = EXCLUDED.archived,
            disabled = EXCLUDED.disabled,
            license_info = EXCLUDED.license_info,
            last_updated = CURRENT_TIMESTAMP
        """
        
        data = []
        for repo in repositories:
            data.append((
                repo.github_id, repo.name, repo.owner_login, repo.full_name,
                repo.description, repo.stargazers_count, repo.forks_count,
                repo.open_issues_count, repo.language, repo.created_at,
                repo.updated_at, repo.pushed_at, repo.size, repo.archived,
                repo.disabled, repo.license_info
            ))
        
        cursor = self.conn.cursor()
        
        try:
            # Get count before operation
            cursor.execute("SELECT COUNT(*) FROM repositories")
            count_before = cursor.fetchone()[0]
            
            # Perform upsert using execute_values for better performance
            execute_values(cursor, insert_query, data)
            self.conn.commit()
            
            # Get count after operation
            cursor.execute("SELECT COUNT(*) FROM repositories")
            count_after = cursor.fetchone()[0]
            
            inserted = count_after - count_before
            updated = len(repositories) - inserted
            
            logger.debug(f"📊 Database: Inserted {inserted}, Updated {updated}")
            return inserted, updated
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Database upsert failed: {e}")
            raise
    
    def bulk_upsert_repositories(self, repositories: List) -> Tuple[int, int]:
        """Ultra-fast bulk upsert for large batches - called by ultra crawler"""
        if not repositories:
            return 0, 0
            
        print(f"💾 Bulk upserting {len(repositories):,} repositories...")
        
        # For very large batches, split into chunks to avoid memory issues
        if len(repositories) > 5000:
            total_inserted = 0
            total_updated = 0
            
            # Process in chunks of 2000
            for i in range(0, len(repositories), 2000):
                chunk = repositories[i:i + 2000]
                inserted, updated = self.upsert_repositories(chunk)
                total_inserted += inserted
                total_updated += updated
                print(f"  ✅ Chunk {i//2000 + 1}: {len(chunk):,} repos")
                
            return total_inserted, total_updated
        else:
            # Use the regular upsert for smaller batches
            return self.upsert_repositories(repositories)
    
    def get_repository_count(self) -> int:
        """Get total number of repositories in database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM repositories")
        return cursor.fetchone()[0]
    
    def get_existing_repository_ids(self, limit: int = 500000) -> set:
        """Get existing repository IDs for duplicate checking"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT github_id FROM repositories LIMIT %s", (limit,))
        return {row[0] for row in cursor.fetchall()}
    
    def export_to_csv(self, filepath: str):
        """Export all repositories to CSV file"""
        try:
            cursor = self.conn.cursor()
            
            # Get all repositories ordered by stars
            cursor.execute("""
                SELECT 
                    github_id, name, owner_login, full_name, description,
                    stargazers_count, forks_count, open_issues_count, language,
                    created_at, updated_at, pushed_at, size, archived,
                    disabled, license_info, crawled_at, last_updated
                FROM repositories 
                ORDER BY stargazers_count DESC
            """)
            
            # Write to CSV
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow([desc[0] for desc in cursor.description])
                
                # Write data rows
                for row in cursor:
                    writer.writerow(row)
            
            # Get count for logging
            count = self.get_repository_count()
            print(f"✅ Exported {count:,} repositories to {filepath}")
            
        except Exception as e:
            logger.error(f"❌ CSV export failed: {e}")
            raise
    
    def export_data(self, format: str = 'csv') -> str:
        """Export repository data to specified format (for backward compatibility)"""
        if format == 'csv':
            return self._export_csv()
        elif format == 'json':
            return self._export_json()
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _export_csv(self) -> str:
        """Export data to CSV string"""
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                github_id, name, owner_login, full_name, description,
                stargazers_count, forks_count, open_issues_count, language,
                created_at, updated_at, pushed_at, size, archived,
                disabled, license_info, crawled_at, last_updated
            FROM repositories
            ORDER BY stargazers_count DESC
        """)
        
        # Write header
        writer.writerow([desc[0] for desc in cursor.description])
        
        # Write data
        for row in cursor:
            writer.writerow(row)
        
        return output.getvalue()
    
    def _export_json(self) -> str:
        """Export data to JSON string"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                github_id, name, owner_login, full_name, description,
                stargazers_count, forks_count, open_issues_count, language,
                created_at, updated_at, pushed_at, size, archived,
                disabled, license_info, crawled_at, last_updated
            FROM repositories
            ORDER BY stargazers_count DESC
        """)
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor:
            results.append(dict(zip(columns, row)))
        
        return json.dumps(results, indent=2, default=str)