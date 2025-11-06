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
    def __init__(self, token: Optional[str] = None, max_concurrent=5):  # Reduced from 25 to 5
        self.token = token or os.getenv('GITHUB_TOKEN')
        self.base_url = "https://api.github.com/graphql"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limit_remaining = 5000
        self.session = None
        self.last_request_time = 0
        self.min_request_interval = 1.0  # 1 second between requests

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _make_request(self, query: str, variables: Dict = None) -> Dict:
        """Make request with rate limit protection"""
        headers = {
            "Authorization": f"Bearer {self.token}" if self.token else "",
            "Content-Type": "application/json",
        }
        
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        # Rate limiting: ensure minimum time between requests
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        
        async with self.semaphore:
            try:
                async with self.session.post(self.base_url, json=payload, headers=headers, timeout=30) as response:
                    self.last_request_time = time.time()
                    
                    if response.status == 200:
                        data = await response.json()
                        
                        # Update rate limit info
                        if 'data' in data and data.get('data') and 'rateLimit' in data['data']:
                            rate_limit_data = data['data']['rateLimit']
                            self.rate_limit_remaining = rate_limit_data['remaining']
                        
                        return data
                    elif response.status == 403:
                        error_text = await response.text()
                        if "secondary rate limit" in error_text:
                            print("⚠️  Secondary rate limit hit! Waiting 5 minutes...")
                            await asyncio.sleep(300)  # Wait 5 minutes
                            raise Exception("Secondary rate limit - wait completed")
                        else:
                            raise Exception(f"HTTP 403: {error_text}")
                    else:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
                        
            except Exception as e:
                if "secondary rate limit" in str(e):
                    raise e  # Re-raise to be handled by caller
                logger.error(f"Request failed: {e}")
                raise

    async def search_repositories_parallel(self, search_queries: List[str], batch_size: int = 100) -> List[Repository]:
        """Execute multiple search queries in parallel with better rate limiting"""
        tasks = []
        for i, query in enumerate(search_queries):
            # Stagger requests to avoid secondary rate limits
            await asyncio.sleep(i * 0.5)  # 0.5 second delay between starting each task
            task = self._search_single_query_safe(query, batch_size)
            tasks.append(task)
        
        # Run with limited concurrency
        results = []
        for i in range(0, len(tasks), 3):  # Process 3 at a time
            batch = tasks[i:i + 3]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            results.extend(batch_results)
            
            # Small delay between batches
            if i + 3 < len(tasks):
                await asyncio.sleep(2)
        
        # Combine all repositories
        all_repositories = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Query failed: {result}")
            elif isinstance(result, list):
                all_repositories.extend(result)
        
        return all_repositories

    async def _search_single_query_safe(self, query: str, batch_size: int) -> List[Repository]:
        """Safe search with pagination and error handling"""
        all_repositories = []
        cursor = None
        pages_fetched = 0
        max_pages = 3  # Limit pages to avoid hitting 1000-result limit too aggressively
        
        while pages_fetched < max_pages:
            try:
                repositories, next_cursor, _ = await self.search_repositories(query, cursor, batch_size)
                if not repositories:
                    break
                
                all_repositories.extend(repositories)
                pages_fetched += 1
                
                if not next_cursor:
                    break
                    
                cursor = next_cursor
                await asyncio.sleep(0.5)  # Delay between pages
                
            except Exception as e:
                if "secondary rate limit" in str(e):
                    print(f"⏸️  Secondary limit for '{query}', skipping...")
                    break
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