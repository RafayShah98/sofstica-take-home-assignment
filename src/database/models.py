import os
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Tuple
from datetime import datetime
import logging

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
CREATE INDEX IF NOT EXISTS idx_repositories_crawled_at ON repositories(crawled_at);
"""

class DatabaseManager:
    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string or os.getenv(
            'DATABASE_URL', 
            'postgresql://postgres:postgres@localhost:5432/github_crawler'
        )
    
    def __enter__(self):
        self.conn = psycopg2.connect(self.connection_string)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def setup_database(self):
        """Create tables and indexes"""
        with self.conn.cursor() as cursor:
            cursor.execute(SCHEMA)
            self.conn.commit()
        logger.info("Database schema created successfully")
    
    def upsert_repositories(self, repositories: List) -> Tuple[int, int]:
        """Upsert repositories and return counts of inserted and updated rows"""
        insert_query = """
        INSERT INTO repositories (
            github_id, name, owner_login, full_name, description, stargazers_count,
            forks_count, open_issues_count, language, created_at, updated_at,
            pushed_at, size, archived, disabled, license_info, last_updated
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
        
        with self.conn.cursor() as cursor:
            # Get count before operation
            cursor.execute("SELECT COUNT(*) FROM repositories")
            count_before = cursor.fetchone()[0]
            
            # Perform upsert
            execute_values(cursor, insert_query, data)
            self.conn.commit()
            
            # Get count after operation
            cursor.execute("SELECT COUNT(*) FROM repositories")
            count_after = cursor.fetchone()[0]
            
            inserted = count_after - count_before
            updated = len(repositories) - inserted
            
            return inserted, updated
    
    def export_data(self, format: str = 'csv') -> str:
        """Export repository data to specified format"""
        query = """
        SELECT 
            github_id, name, owner_login, full_name, description,
            stargazers_count, forks_count, open_issues_count, language,
            created_at, updated_at, pushed_at, size, archived,
            disabled, license_info, crawled_at, last_updated
        FROM repositories
        ORDER BY stargazers_count DESC
        """
        
        if format == 'csv':
            return self._export_csv(query)
        elif format == 'json':
            return self._export_json(query)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    def _export_csv(self, query: str) -> str:
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            writer.writerow(columns)
            
            for row in cursor:
                writer.writerow(row)
        
        return output.getvalue()
    
    def _export_json(self, query: str) -> str:
        import json
        
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor:
                results.append(dict(zip(columns, row)))
        
        return json.dumps(results, indent=2, default=str)