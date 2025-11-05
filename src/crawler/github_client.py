import os
import time
import requests
from typing import List, Dict, Optional
from dataclasses import dataclass
import logging

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
        
    def _make_request(self, query: str, cursor: Optional[str] = None) -> Dict:
        headers = {
            "Authorization": f"Bearer {self.token}" if self.token else "",
            "Content-Type": "application/json",
        }
        
        variables = {}
        if cursor:
            variables["cursor"] = cursor
            
        payload = {
            "query": query,
            "variables": variables
        }
        
        while True:
            try:
                response = requests.post(self.base_url, json=payload, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Update rate limit info from headers
                    if 'X-RateLimit-Remaining' in response.headers:
                        self.rate_limit_remaining = int(response.headers['X-RateLimit-Remaining'])
                    if 'X-RateLimit-Reset' in response.headers:
                        self.rate_limit_reset = int(response.headers['X-RateLimit-Reset'])
                    
                    if 'errors' in data:
                        for error in data['errors']:
                            if 'type' in error and error['type'] == 'RATE_LIMITED':
                                sleep_time = self.rate_limit_reset - time.time() + 10
                                if sleep_time > 0:
                                    logger.warning(f"Rate limited. Sleeping for {sleep_time} seconds")
                                    time.sleep(sleep_time)
                                continue
                            else:
                                raise Exception(f"GraphQL error: {error}")
                    
                    return data
                    
                elif response.status_code == 403:
                    # Rate limited
                    sleep_time = self.rate_limit_reset - time.time() + 10
                    if sleep_time > 0:
                        logger.warning(f"Rate limited. Sleeping for {sleep_time} seconds")
                        time.sleep(sleep_time)
                    continue
                    
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                time.sleep(60)  # Wait before retrying
                continue
    
    def get_repositories_batch(self, cursor: Optional[str] = None, batch_size: int = 50) -> tuple[List[Repository], Optional[str]]:
        query = """
        query($cursor: String) {
          search(
            query: "stars:>1 sort:updated-desc"
            type: REPOSITORY
            first: %d
            after: $cursor
          ) {
            edges {
              node {
                ... on Repository {
                  id
                  name
                  owner {
                    login
                  }
                  nameWithOwner
                  description
                  stargazerCount
                  forkCount
                  issues(states: OPEN) {
                    totalCount
                  }
                  primaryLanguage {
                    name
                  }
                  createdAt
                  updatedAt
                  pushedAt
                  diskUsage
                  isArchived
                  isDisabled
                  licenseInfo {
                    key
                  }
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
        
        data = self._make_request(query, cursor)
        
        if not data or 'data' not in data:
            logger.error("Invalid response data")
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