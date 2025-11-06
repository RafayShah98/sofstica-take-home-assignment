# GitHub Repository Crawler

A Python application that crawls GitHub repositories using GraphQL API and stores data in PostgreSQL.

## Features

- Crawls repository data including stars, forks, issues, etc.
- Stores data in PostgreSQL with efficient upsert operations
- Respects GitHub API rate limits with retry mechanisms
- Daily automated runs via GitHub Actions
- Export data to CSV/JSON formats
- **Works with GitHub's default token (no secrets required)**
- **Service container setup for PostgreSQL in CI/CD**

## Setup

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up PostgreSQL database
4. Run: `python main.py`

## GitHub Actions

The workflow runs daily and includes:
- ✅ PostgreSQL service container
- ✅ Automated database setup
- ✅ Repository crawling (100,000 repos target)
- ✅ Data export and artifact upload
- ✅ Works with default GitHub token
