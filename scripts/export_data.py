import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database.models import DatabaseManager

def export_data():
    """Export data to CSV and JSON files"""
    with DatabaseManager() as db:

        csv_data = db.export_data('csv')
        with open('repositories_export.csv', 'w', newline='', encoding='utf-8') as f:
            f.write(csv_data)
        print("Data exported to repositories_export.csv")
        
        json_data = db.export_data('json')
        with open('repositories_export.json', 'w', encoding='utf-8') as f:
            f.write(json_data)
        print("Data exported to repositories_export.json")

if __name__ == "__main__":
    export_data()