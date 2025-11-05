import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database.setup import setup_database

if __name__ == "__main__":
    setup_database()