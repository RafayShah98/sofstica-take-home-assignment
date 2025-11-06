# config.py
import os

# Database configuration
DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/github_crawler'

# 🔑 Hardcode multiple GitHub tokens directly here
# Replace these sample tokens with your real tokens from GitHub
GITHUB_TOKENS = [
    "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",
    "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",
    "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",
    "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",
    "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",
    "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",
]

# Optional: log how many tokens were detected
print(f"🔑 Loaded {len(GITHUB_TOKENS)} GitHub token(s).")

# (Optional) Safety check
if not GITHUB_TOKENS:
    raise ValueError("❌ No GitHub tokens found! Please define them in config.py")
