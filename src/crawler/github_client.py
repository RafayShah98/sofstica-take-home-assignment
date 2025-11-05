import os
import time
import requests
import asyncio
import aiohttp
from typing import List, Dict, Optional
from dataclasses import dataclass
import logging
from concurrent.futures import ThreadPoolExecutor
import json

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

class GitHubClient:
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv('GITHUB_TOKEN')
        if not self.token:
            logger.warning("No GitHub token provided. Using unauthenticated requests (lower rate limits)")
        self.base_url = "https://api.github.com/graphql"
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = 0
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}" if self.token else "",
            "Content-Type": "application/json",
        })
        
    def _make_request(self, query: str, variables: Dict = None) -> Dict:
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                start_time = time.time()
                response = self.session.post(self.base_url, json=payload, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    request_time = time.time() - start_time
                    
                    # Update rate limit info
                    if 'X-RateLimit-Remaining' in response.headers:
                        self.rate_limit_remaining = int(response.headers['X-RateLimit-Remaining'])
                    if 'X-RateLimit-Reset' in response.headers:
                        self.rate_limit_reset = int(response.headers['X-RateLimit-Reset'])
                    
                    if 'errors' in data:
                        for error in data['errors']:
                            if 'type' in error and error['type'] == 'RATE_LIMITED':
                                sleep_time = self.rate_limit_reset - time.time() + 5
                                if sleep_time > 0:
                                    logger.warning(f"Rate limited. Sleeping for {sleep_time:.1f}s")
                                    time.sleep(sleep_time)
                                continue
                    logger.debug(f"Request completed in {request_time:.2f}s, Rate limit: {self.rate_limit_remaining}")
                    return data
                    
                elif response.status_code == 403:
                    sleep_time = self.rate_limit_reset - time.time() + 5
                    if sleep_time > 0:
                        logger.warning(f"Rate limited. Sleeping for {sleep_time:.1f}s")
                        time.sleep(sleep_time)
                    continue
                    
                else:
                    logger.error(f"HTTP {response.status_code}")
                    if response.status_code >= 500:
                        retry_count += 1
                        sleep_time = 2 ** retry_count
                        logger.warning(f"Server error, retry {retry_count}/{max_retries} in {sleep_time}s")
                        time.sleep(sleep_time)
                        continue
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                retry_count += 1
                sleep_time = 2 ** retry_count
                logger.error(f"Request failed (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    time.sleep(sleep_time)
                else:
                    raise
        
        raise Exception("Max retries exceeded")

    def get_repositories_parallel(self, search_queries: List[str], batch_size: int = 100) -> List[Repository]:
        """Get repositories using parallel requests for different search criteria"""
        all_repositories = []
        
        def fetch_for_query(query):
            repositories = []
            cursor = None
            query_start = time.time()
            
            while len(repositories) < 1000:  # GitHub's limit per query
                try:
                    batch, cursor = self._get_repositories_batch(query, cursor, batch_size)
                    if not batch:
                        break
                    repositories.extend(batch)
                    
                    if not cursor:
                        break
                        
                    # Small delay between batches
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error in query '{query}': {e}")
                    break
            
            query_time = time.time() - query_start
            logger.info(f"Query '{query}' returned {len(repositories)} repos in {query_time:.1f}s")
            return repositories
        
        # Use ThreadPoolExecutor for parallel execution
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(fetch_for_query, query) for query in search_queries]
            for future in futures:
                try:
                    all_repositories.extend(future.result(timeout=300))  # 5min timeout per query
                except Exception as e:
                    logger.error(f"Query failed: {e}")
        
        return all_repositories

    def _get_repositories_batch(self, search_query: str, cursor: Optional[str] = None, batch_size: int = 100) -> tuple[List[Repository], Optional[str]]:
        query = """
        query($cursor: String, $query: String!) {
          search(
            query: $query
            type: REPOSITORY
            first: %d
            after: $cursor
          ) {
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
        }
        """ % batch_size
        
        variables = {"query": search_query}
        if cursor:
            variables["cursor"] = cursor
        
        data = self._make_request(query, variables)
        
        if not data or 'data' not in data:
            return [], None
            
        search_data = data['data']['search']
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
        
        return repositories, next_cursor