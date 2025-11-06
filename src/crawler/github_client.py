import os
import time
import requests
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging
from datetime import datetime, timezone
import itertools
from config import GITHUB_TOKENS  # 👈 integrates your config.py

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
    def __init__(self):
        """
        Initialize GitHubClient with automatic token rotation support.
        Tokens are loaded from config.GITHUB_TOKENS.
        """
        # Load all tokens from config
        self.tokens = GITHUB_TOKENS
        self.token_cycle = itertools.cycle(self.tokens)
        self.token = next(self.token_cycle)

        logger.info(f"🔑 Loaded {len(self.tokens)} GitHub token(s) for rotation.")

        self.base_url = "https://api.github.com/graphql"
        self.rate_limit_remaining = 5000
        self.rate_limit_reset = 0
        self.last_query_cost = 1  # Default cost

    # ============================================================
    # Token Rotation Logic
    # ============================================================
    def _rotate_token(self):
        """Rotate to the next token in the pool when rate-limited."""
        self.token = next(self.token_cycle)
        logger.info("🔄 Switched to next GitHub token due to rate limit.")

    # ============================================================
    # Core Request Logic
    # ============================================================
    def _to_repository(self, node: Dict) -> Repository:
        return Repository(
            github_id=node.get('id', ''),
            name=node.get('name', '') if 'name' in node else node.get('nameWithOwner', '').split('/')[-1],
            owner_login=node.get('owner', {}).get('login', '') if 'owner' in node else node.get('nameWithOwner', '').split('/')[0],
            full_name=node.get('nameWithOwner', ''),
            description=node.get('description'),
            stargazers_count=node.get('stargazerCount', 0),
            forks_count=node.get('forkCount', 0),
            open_issues_count=node.get('issues', {}).get('totalCount', 0) if 'issues' in node else 0,
            language=node.get('primaryLanguage', {}).get('name') if node.get('primaryLanguage') else None,
            created_at=node.get('createdAt', ''),
            updated_at=node.get('updatedAt', ''),
            pushed_at=node.get('pushedAt', ''),
            size=node.get('diskUsage', 0),
            archived=node.get('isArchived', False),
            disabled=node.get('isDisabled', False),
            license_info=node.get('licenseInfo', {}).get('key') if node.get('licenseInfo') else None
        )

    def _make_request(self, query: str, variables: Dict = None) -> Dict:
        headers = {
            "Authorization": f"Bearer {self.token}" if self.token else "",
            "Content-Type": "application/json",
        }

        payload = {"query": query, "variables": variables or {}}

        retry_count = 0
        max_retries = 5

        while retry_count < max_retries:
            try:
                response = requests.post(self.base_url, json=payload, headers=headers, timeout=60)

                # --- 200 OK ---
                if response.status_code == 200:
                    data = response.json()

                    # Update rate limit info
                    rate_limit_data = data.get('data', {}).get('rateLimit')
                    if rate_limit_data:
                        self.rate_limit_remaining = rate_limit_data['remaining']
                        self.last_query_cost = rate_limit_data['cost']
                        reset_at_utc = datetime.strptime(rate_limit_data['resetAt'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
                        self.rate_limit_reset = reset_at_utc.timestamp()

                    # Handle GraphQL rate limit error
                    if 'errors' in data:
                        rate_limit_hit = any(err.get('type') == 'RATE_LIMITED' for err in data['errors'])
                        if rate_limit_hit:
                            self._handle_rate_limit()
                            continue
                        else:
                            logger.error(f"GraphQL errors: {data['errors']}")
                            raise Exception(f"GraphQL query failed: {data['errors']}")

                    return data

                # --- 403 Forbidden (rate-limited or abuse detection) ---
                elif response.status_code == 403:
                    logger.warning("⚠️ HTTP 403 rate limit or abuse detection triggered.")
                    self._handle_rate_limit()
                    continue

                # --- 5xx Server Errors ---
                elif response.status_code >= 500:
                    retry_count += 1
                    backoff = 2 ** retry_count
                    logger.warning(f"Server error {response.status_code}. Retrying in {backoff}s...")
                    time.sleep(backoff)
                    continue

                # --- Other Errors ---
                else:
                    logger.error(f"HTTP {response.status_code}: {response.text}")
                    response.raise_for_status()

            except requests.exceptions.RequestException as e:
                retry_count += 1
                backoff = 2 ** retry_count
                logger.error(f"Request failed ({retry_count}/{max_retries}): {e}")
                time.sleep(backoff)
                if retry_count >= max_retries:
                    raise

        raise Exception("Max retries exceeded for GitHub API request")

    # ============================================================
    # Rate Limit Handling
    # ============================================================
    def _handle_rate_limit(self):
        """Handle rate limit: rotate token or sleep until reset."""
        if len(self.tokens) > 1:
            logger.warning("🔁 Rate limit hit — rotating to next token.")
            self._rotate_token()
        else:
            reset_in = self.rate_limit_reset - time.time() + 15
            if reset_in > 0:
                logger.warning(f"Rate limit reached. Sleeping {reset_in:.0f}s until reset.")
                time.sleep(reset_in)

    # ============================================================
    # GraphQL Search Queries
    # ============================================================
    def search_repositories(self, query: str, cursor: Optional[str] = None, batch_size: int = 100) -> Tuple[List[Repository], Optional[str], Dict]:
        graphql_query = f"""
        query($cursor: String, $query: String!) {{
          search(query: $query, type: REPOSITORY, first: {batch_size}, after: $cursor) {{
            repositoryCount
            edges {{
              node {{
                ... on Repository {{
                  id
                  name
                  owner {{ login }}
                  nameWithOwner
                  description
                  stargazerCount
                  forkCount
                  issues(states: OPEN) {{ totalCount }}
                  primaryLanguage {{ name }}
                  createdAt
                  updatedAt
                  pushedAt
                  diskUsage
                  isArchived
                  isDisabled
                  licenseInfo {{ key }}
                }}
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
          rateLimit {{
            cost
            remaining
            resetAt
          }}
        }}
        """

        variables = {"query": query, "cursor": cursor}
        data = self._make_request(graphql_query, variables)

        if not data or 'data' not in data:
            logger.error("Invalid response data from GitHub.")
            return [], None, {}

        search_data = data['data']['search']
        rate_limit_info = data['data'].get('rateLimit', {})

        repos = [self._to_repository(edge['node']) for edge in search_data['edges']]
        page_info = search_data['pageInfo']
        next_cursor = page_info['endCursor'] if page_info['hasNextPage'] else None

        return repos, next_cursor, rate_limit_info

    # Convenience wrappers
    def get_repositories_by_stars_range(self, min_stars: int, max_stars: int, cursor: Optional[str] = None, batch_size: int = 100):
        stars_query = f"stars:{min_stars}..{max_stars}" if min_stars != max_stars else f"stars:{min_stars}"
        return self.search_repositories(f"{stars_query} sort:updated-desc", cursor, batch_size)

    def get_repositories_by_date(self, date_str: str, cursor: Optional[str] = None, batch_size: int = 100):
        return self.search_repositories(f"created:{date_str} sort:updated-desc", cursor, batch_size)

    def get_repositories_by_language(self, language: str, cursor: Optional[str] = None, batch_size: int = 100):
        return self.search_repositories(f"language:{language} stars:>1 sort:updated-desc", cursor, batch_size)

    def get_repositories_by_stars(self, min_stars: int = 1, cursor: Optional[str] = None, batch_size: int = 100):
        return self.search_repositories(f"stars:>={min_stars} sort:updated-desc", cursor, batch_size)
