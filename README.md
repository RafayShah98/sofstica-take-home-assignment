# GitHub Repository Crawler

A Python application that crawls GitHub repositories using GraphQL API and stores data in PostgreSQL.

## Features

- Crawls repository data including stars, forks, issues, etc.
- Stores data in PostgreSQL with efficient upsert operations
- Respects GitHub API rate limits with retry mechanisms
- Daily automated runs via GitHub Actions
- Export data to CSV/JSON formats

## Setup

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up PostgreSQL database
4. Run: `python src/main.py`

## GitHub Actions

The workflow runs daily at 2 AM UTC and can be manually triggered.
