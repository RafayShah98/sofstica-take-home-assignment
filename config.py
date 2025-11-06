import os
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/github_crawler')

# Load multiple GitHub tokens if provided
# Supports either:
#   - GITHUB_TOKENS=token1,token2,token3  (comma-separated)
#   - GITHUB_TOKEN=single_token
tokens_env = os.getenv("GITHUB_TOKENS")
if tokens_env:
    # Split by comma and strip whitespace
    GITHUB_TOKENS = [token.strip() for token in tokens_env.split(",") if token.strip()]
else:
    single_token = os.getenv("GITHUB_TOKEN")
    if single_token:
        GITHUB_TOKENS = [single_token]
    else:
        raise ValueError("❌ No GitHub token(s) found. Please set GITHUB_TOKENS or GITHUB_TOKEN.")

# Optional: log how many tokens were detected
print(f"🔑 Loaded {len(GITHUB_TOKENS)} GitHub token(s).")
