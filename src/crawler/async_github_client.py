import aiohttp
import asyncio
import os
import time
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

@dataclass
class Repository:
    github_id: str
    name: str
    owner_login: str
    full_name: str
    description: Optional[str]
    stargazers_count: int
    forks_count: int
    open_issues_count: int
    language: Optional[str]
    created_at: str
    updated_at: str
    pushed_at: str
    size: int
    archived: bool
    disabled: bool
    license_info: Optional[str]

class AsyncGitHubClient:
    def __init__(self, token: Optional[str] = None, max_concurrent=10):
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.base_url = "https://api.github.com/graphql"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limit_remaining = 5000
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, query: str, variables: Dict = None) -> Dict:
        headers = {
            "Authorization": f"Bearer {self.token}" if self.token else "",
            "Content-Type": "application/json",
        }
        
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        async with self.semaphore:
            async with self.session.post(self.base_url, json=payload, headers=headers, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Update rate limit info
                    if 'data' in data and data.get('data') and 'rateLimit' in data['data']:
                        rate_limit_data = data['data']['rateLimit']
                        self.rate_limit_remaining = rate_limit_data['remaining']
                    
                    return data
                else:
                    raise Exception(f"HTTP {response.status}: {await response.text()}")

    async def search_repositories_parallel(self, search_queries: List[str], batch_size: int = 100) -> List[Repository]:
        """Execute multiple search queries in parallel"""
        tasks = []
        for query in search_queries:
            task = self._search_single_query(query, batch_size)
            tasks.append(task)
        
        # Run all searches in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine all repositories
        all_repositories = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Query failed: {result}")
            else:
                all_repositories.extend(result)
        
        return all_repositories

    async def _search_single_query(self, query: str, batch_size: int) -> List[Repository]:
        """Search repositories for a single query with pagination"""
        all_repositories = []
        cursor = None
        
        while len(all_repositories) < 1000:  # GitHub's limit per query
            try:
                repositories, next_cursor, _ = await self.search_repositories(query, cursor, batch_size)
                if not repositories:
                    break
                
                all_repositories.extend(repositories)
                
                if not next_cursor:
                    break
                    
                cursor = next_cursor
                await asyncio.sleep(0.1)  # Small delay between pages
                
            except Exception as e:
                logger.error(f"Error in query '{query}': {e}")
                break
        
        return all_repositories

    async def search_repositories(self, query: str, cursor: Optional[str] = None, batch_size: int = 100) -> Tuple[List[Repository], Optional[str], Dict]:
        """Single repository search"""
        graphql_query = """
        query($cursor: String, $query: String!) {
          search(
            query: $query
            type: REPOSITORY
            first: %d
            after: $cursor
          ) {
            repositoryCount
            edges {
              node {
                ... on Repository {
                  id
                  name
                  owner { login }
                  nameWithOwner
                  description
                  stargazerCount
                  forkCount
                  issues(states: OPEN) { totalCount }
                  primaryLanguage { name }
                  createdAt
                  updatedAt
                  pushedAt
                  diskUsage
                  isArchived
                  isDisabled
                  licenseInfo { key }
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
          rateLimit {
            cost
            remaining
            resetAt
          }
        }
        """ % batch_size
        
        variables = {"query": query, "cursor": cursor}
        data = await self._make_request(graphql_query, variables)
        
        if not data or 'data' not in data:
            return [], None, {}
            
        search_data = data['data']['search']
        rate_limit_info = data['data'].get('rateLimit', {})
        
        repositories = []
        for edge in search_data['edges']:
            node = edge['node']
            repo = Repository(
                github_id=node['id'],
                name=node['name'],
                owner_login=node['owner']['login'],
                full_name=node['nameWithOwner'],
                description=node['description'],
                stargazers_count=node['stargazerCount'],
                forks_count=node['forkCount'],
                open_issues_count=node['issues']['totalCount'],
                language=node['primaryLanguage']['name'] if node['primaryLanguage'] else None,
                created_at=node['createdAt'],
                updated_at=node['updatedAt'],
                pushed_at=node['pushedAt'],
                size=node['diskUsage'],
                archived=node['isArchived'],
                disabled=node['isDisabled'],
                license_info=node['licenseInfo']['key'] if node['licenseInfo'] else None
            )
            repositories.append(repo)
        
        page_info = search_data['pageInfo']
        next_cursor = page_info['endCursor'] if page_info['hasNextPage'] else None
        
        return repositories, next_cursor, rate_limit_info