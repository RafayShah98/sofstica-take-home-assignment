import os
import time
import requests
from typing import List, Dict, Optional
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

class GitHubClient:
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv('GITHUB_TOKEN')
        if not self.token:
            logger.warning("No GitHub token provided. Using unauthenticated requests (lower rate limits)")
        self.base_url = "https://api.github.com/graphql"
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = 0
        self.last_query_cost = 1  # Default cost

    def _make_request(self, query: str, variables: Dict = None) -> Dict:
        headers = {
            "Authorization": f"Bearer {self.token}" if self.token else "",
            "Content-Type": "application/json",
        }
        
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                response = requests.post(self.base_url, json=payload, headers=headers, timeout=60)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Update rate limit info from GraphQL response if available (more accurate)
                    if 'data' in data and data.get('data') and 'rateLimit' in data['data'] and data['data']['rateLimit']:
                        rate_limit_data = data['data']['rateLimit']
                        self.rate_limit_remaining = rate_limit_data['remaining']
                        self.last_query_cost = rate_limit_data['cost']
                        # Parse UTC timestamp
                        reset_at_utc = datetime.strptime(rate_limit_data['resetAt'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                        self.rate_limit_reset = reset_at_utc.timestamp()
                    # Fallback to headers if GraphQL data is missing
                    elif 'X-RateLimit-Remaining' in response.headers:
                        self.rate_limit_remaining = int(response.headers['X-RateLimit-Remaining'])
                        if 'X-RateLimit-Reset' in response.headers:
                            self.rate_limit_reset = int(response.headers['X-RateLimit-Reset'])

                    if 'errors' in data:
                        rate_limit_hit = False
                        for error in data['errors']:
                            if error.get('type') == 'RATE_LIMITED':
                                rate_limit_hit = True
                                break  # Found the rate limit error
                            else:
                                logger.error(f"GraphQL error: {error}")
                        
                        if rate_limit_hit:
                            sleep_time = self.rate_limit_reset - time.time() + 15  # 15s buffer
                            if sleep_time > 0:
                                logger.warning(f"Hard rate limit hit. Sleeping for {sleep_time:.1f} seconds until reset.")
                                time.sleep(sleep_time)
                            # Now continue the while loop to retry the request
                            continue
                        else:
                            # If there were errors but none were rate limit errors
                            raise Exception(f"GraphQL query failed with errors: {data['errors']}")
                    
                    return data
                    
                elif response.status_code == 403:
                    # This could also indicate a rate limit issue
                    sleep_time = self.rate_limit_reset - time.time() + 15
                    if self.rate_limit_reset > 0 and sleep_time > 0:
                        logger.warning(f"Received HTTP 403. Potentially rate-limited. Sleeping for {sleep_time:.1f} seconds.")
                        time.sleep(sleep_time)
                    else:
                        # If we don't have reset info, do an exponential backoff
                        retry_count += 1
                        sleep_time = (2 ** retry_count)
                        logger.warning(f"Received HTTP 403. Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                    continue
                    
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    if response.status_code >= 500:
                        # Server error, retry with backoff
                        retry_count += 1
                        sleep_time = 2 ** retry_count  # Exponential backoff
                        logger.warning(f"Server error, retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
                        continue
                    else:
                        response.raise_for_status()
                    
            except requests.exceptions.RequestException as e:
                retry_count += 1
                sleep_time = 2 ** retry_count  # Exponential backoff
                logger.error(f"Request failed (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error("Max retries exceeded")
                    raise
        
        raise Exception("Max retries exceeded for GitHub API request")
    
    def get_repositories_by_stars_range(self, min_stars: int, max_stars: int, cursor: Optional[str] = None, batch_size: int = 100) -> tuple[List[Repository], Optional[str]]:
        """Get repositories by stars range to work around 1000 result limit"""
        if min_stars == max_stars:
            stars_query = f"stars:{min_stars}"
        else:
            stars_query = f"stars:{min_stars}..{max_stars}"
        
        query = """
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
        
        search_query = f"{stars_query} sort:updated-desc"
        variables = {
            "query": search_query,
            "cursor": cursor
        }
        
        data = self._make_request(query, variables)
        
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
        
        if 'repositoryCount' in search_data:
            logger.debug(f"Found {search_data['repositoryCount']} repositories for stars {min_stars}-{max_stars}")
        
        return repositories, next_cursor
    
    def get_repositories_by_date(self, date_str: str, cursor: Optional[str] = None, batch_size: int = 100) -> tuple[List[Repository], Optional[str]]:
        """Get repositories created on a specific date"""
        query = """
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
        
        search_query = f"created:{date_str} sort:updated-desc"
        variables = {
            "query": search_query,
            "cursor": cursor
        }
        
        data = self._make_request(query, variables)
        
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
        
        if 'repositoryCount' in search_data:
            logger.debug(f"Found {search_data['repositoryCount']} repositories for date {date_str}")
            
        return repositories, next_cursor

    def get_repositories_by_language(self, language: str, cursor: Optional[str] = None, batch_size: int = 100) -> tuple[List[Repository], Optional[str]]:
        """Get repositories by programming language"""
        query = """
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
        
        search_query = f"language:{language} stars:>1 sort:updated-desc"
        variables = {
            "query": search_query,
            "cursor": cursor
        }
        
        data = self._make_request(query, variables)
        
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