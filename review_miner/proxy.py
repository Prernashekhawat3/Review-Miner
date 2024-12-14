import os
import logging
from urllib.parse import urlencode
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class ProxyManager:
    """
    Manages the generation of proxy URLs for different proxy providers.
    """
    def __init__(self):
        # Initialize API keys from environment variables
        self.api_keys = {
            "scraperapi": os.getenv("SCRAPER_API_KEY"),
            "scrapeops": os.getenv("SCRAPEOPS_API_KEY"),
        }
    
    def get_proxy_url(self, url, proxy_name):
 
        try:
            if proxy_name not in self.api_keys or not self.api_keys[proxy_name]:
                raise ValueError(f"API key for {proxy_name} is not configured.")
            
            # Strip the URL to ensure no trailing spaces
            striped_url = url.strip()
            
            # Payload for proxy request
            payload = {"url": striped_url, "api_key": self.api_keys[proxy_name],"country_code":"US"}
            
            # Base URLs for supported proxy providers
            base_urls = {
                "scraperapi": "http://api.scraperapi.com/",
                "scrapeops": "https://proxy.scrapeops.io/v1/",
            }
            
            if proxy_name not in base_urls:
                raise ValueError(f"Unsupported proxy provider: {proxy_name}")
            
            # Generate the proxy URL
            proxy_url = base_urls[proxy_name] + "?" + urlencode(payload)
            logging.info(f"Generated {proxy_name.capitalize()} URL: {proxy_url}")
            return proxy_url
        
        except Exception as e:
            logging.error(f"Error generating proxy URL for {proxy_name}: {e}")
            return None
