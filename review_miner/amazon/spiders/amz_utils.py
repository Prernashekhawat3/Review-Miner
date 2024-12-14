import os
import logging
import re
from urllib.parse import urlparse


def log_response_info(response):
    """Logs response status and a snippet of the response body."""
    logging.info(f"Response status: {response.status} for URL: {response.url}")
    logging.debug(f"Response body snippet: {response.text[:500]}")


def detect_page_template(response, logger, page_count):
    # Detect page template type
    if response.css('.a-section.a-spacing-small > .puisg-row'):
        # Check the number of products to differentiate between General and Horizontal templates
        product_list = response.css('.a-section.a-spacing-small > .puisg-row')
        if len(product_list) < 5:
            # Template 1 - General
            template_type = "Template 1 - General"
        else:
            # Template 2 - Horizontal
            template_type = "Template 2 - Horizontal"
    elif response.css('.a-section > .puisg-row'):
        # Template 3 - Vertical
        product_list = response.css('.a-section > .puisg-row')
        template_type = "Template 2 - Horizontal"
    else:
        # Fallback to an alternative vertical template if neither is found
        product_list = response.css('.puis-card-border')
        template_type = "Template 4 - Vertical "

    print("=======", template_type)
    logger.info(f"Using {template_type}. Found {len(product_list)} products on page {page_count} of {response.url}")
    
    return template_type, product_list

def extract_asin(self, url): 
    path = urlparse(url).path
    if path.startswith('/dp'):
        asin = path.split('/')[2]
    else:
        asin = path.split('/')[3]
    return asin

