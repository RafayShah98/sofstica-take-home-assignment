from .models import DatabaseManager
import logging

logging.basicConfig(level=logging.INFO)

def setup_database():
    """Initialize database schema"""
    with DatabaseManager() as db:
        db.setup_database()

if __name__ == "__main__":
    setup_database()