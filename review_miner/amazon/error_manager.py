import os
import hashlib
import pandas as pd
import logging
from datetime import datetime
 
class ErrorManager:
    def __init__(self, task_id, scraper_name):
        self.file_name = f"Error__{task_id}__{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')}.csv"
        self.file_path = os.path.join(os.getcwd(), 'data', self.file_name)
        self.task_id = task_id
        self.scraper_name = scraper_name
        self.ensure_directory_exists()
        
    def ensure_directory_exists(self):
        """Ensure the 'data' directory exists for storing logs."""
        os.makedirs(os.path.join(os.getcwd(), 'data'), exist_ok=True)
 
    def log_error(self, error_data):
        """Log an error to the CSV file."""
        file_exists = os.path.isfile(self.file_path)
        df = pd.DataFrame([error_data])
        try:
            df.to_csv(self.file_path, header=not file_exists, mode="a", index=False)
            logging.info(f"Error data written to {self.file_path}")
        except Exception as e:
            logging.error(f"Failed to log error to CSV: {e}")
 
    def create_error_data(self, request_url, error_name, error_description, exception=None):
        """Generate a standardized error record."""
        return {
            'error_id': hashlib.md5((request_url + error_name).encode()).hexdigest(),
            'task_id': self.task_id,
            'request_url': request_url,
            'error_category': self.categorize_error(error_name),
            'error_name': error_name,
            'error_description': error_description,
            'scraper_name': self.scraper_name,
            'date': datetime.utcnow().strftime("%Y-%m-%d"),
            'time': datetime.utcnow().strftime("%H:%M:%S"),
            'exception': str(exception) if exception else None
        }
 
    def categorize_error(self, error_name):
        """Map error names to categories."""
        if 'Timeout' in error_name or 'Connection' in error_name:
            return 'Network'
        elif 'Response' in error_name or 'Status' in error_name:
            return 'Response'
        elif 'Parsing' in error_name:
            return 'Parsing'
        elif 'Scraper' in error_name:
            return 'ScraperConfiguration'
        return 'General'