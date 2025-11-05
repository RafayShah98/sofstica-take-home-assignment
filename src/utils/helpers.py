import json
from datetime import datetime

def format_timestamp(timestamp: str) -> str:
    """Format ISO timestamp to readable string"""
    try:
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return timestamp

def save_json(data, filename: str):
    """Save data as JSON file"""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2, default=str)

def load_json(filename: str):
    """Load data from JSON file"""
    with open(filename, 'r') as f:
        return json.load(f)