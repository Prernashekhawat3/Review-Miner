# Importing For Scrapy Signals
import csv , logging,requests,os,hashlib , requests,shutil
import scrapy
from scrapy import signals 
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError

# Importing For Error Management
import pandas as pd 
import os
from datetime import datetime
from enum import Enum

class ErrorType(Enum):
    NETWORK_ERROR = "Network Error"
    PARSING_ERROR = "Parsing Error"
    RESPONSE_ERROR = "Response Error"
    SCRAPER_ERROR = "Scraper Error"
    GENERAL_ERROR = "General Error"

class ErrorReason(Enum):
    # Network Errors
    DNS_LOOKUP_FAILURE = 101
    TIMEOUT = 102
    CONNECTION_ERROR = 103

    # Parsing Errors
    MISSING_ATTRIBUTE = 201
    UNEXPECTED_STRUCTURE = 202

    # Response Errors
    NOT_FOUND = 301
    SERVER_ERROR = 302
    INVALID_RESPONSE = 303
    UNEXPECTED_STATUS = 304

    # Scraper Errors
    INVALID_API_KEY = 401
    QUOTA_EXCEEDED = 402

    # General Errors
    UNKNOWN_ERROR = 501


class ErrorManager:
    def __init__(self, task_id, celery_id, scraper_name):
        self.file_name = f"Error__{task_id}__{celery_id}__{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')}.csv"
        self.file_path = os.path.join(os.getcwd(), 'data', self.file_name)
        self.task_id = task_id
        self.celery_id = celery_id
        self.scraper_name = scraper_name
        self.max_error_code = None
        self.ensure_directory_exists()

    def ensure_directory_exists(self):
        """Ensure the 'data' directory exists for storing logs."""
        os.makedirs(os.path.join(os.getcwd(), 'data'), exist_ok=True)

    def log_error(self, error_type: ErrorType, error_reason: ErrorReason, request_url, proxy_url=None, response=None, exception=None, meta=None):
        """Centralized error logging method."""
        file_exists = os.path.isfile(self.file_path)

        # Swap error_code and error_reason_description
        error_code = error_reason.value  # Use the value of error_reason as the error code
        error_reason_description = error_reason.name  # Use the name of error_reason as the description

        # Update max_error_code
        if not self.max_error_code or error_code < self.max_error_code:
            self.max_error_code = error_code

        # Prepare error data for logging
        error_data = {
            'error_code': error_code,  # Numeric code
            'error_type': error_type.value, 
            'error_reason': error_reason_description,  # Reason description (e.g., "MISSING_ATTRIBUTE")
            'error_id': hashlib.md5((request_url + error_reason_description).encode()).hexdigest(),
            'task_id': self.task_id,
            'celery_id': self.celery_id,
            'request_url': request_url,
            'proxy_url': proxy_url,
            'scraper_name': self.scraper_name,
            'status_code': getattr(response, 'status', None) if response else None,
            'response_url': getattr(response, 'url', None) if response else None,
            'date': datetime.utcnow().strftime("%Y-%m-%d"),
            'time': datetime.utcnow().strftime("%H:%M:%S"),
            'exception': str(exception) if exception else None
        }

        # Write error data to a CSV file
        df = pd.DataFrame([error_data])
        try:
            df.to_csv(self.file_path, header=not file_exists, mode="a", index=False)
            logging.info(f"Error logged: {error_type.value} - {error_reason_description}")
        except Exception as e:
            logging.error(f"Failed to log error to CSV: {e}")


    def handle_error(self, error_mapping, request_url,proxy_url=None, response=None, exception=None, meta=None):
        """Generic error handler that logs errors based on a mapping."""
        error_type, error_reason = error_mapping
        self.log_error(
            error_type=error_type,
            error_reason=error_reason,
            request_url=request_url,
            proxy_url = proxy_url,
            response=response,
            exception=exception,
            meta=meta
        )

    def handle_network_error(self, failure):
        """Handle network-related errors."""
        error_mapping = None
        if failure.check(DNSLookupError):
            error_mapping = (ErrorType.NETWORK_ERROR, ErrorReason.DNS_LOOKUP_FAILURE)
        elif failure.check(TimeoutError, TCPTimedOutError):
            error_mapping = (ErrorType.NETWORK_ERROR, ErrorReason.TIMEOUT)
        else:
            error_mapping = (ErrorType.NETWORK_ERROR, ErrorReason.CONNECTION_ERROR)

        self.handle_error(
            error_mapping,
            request_url=failure.request.meta.get('original_url', failure.request.url),  # Use original URL
            proxy_url=failure.request.url,  # Use proxied URL
            exception=failure.value
        )

    def handle_response_errors(self, response):
        """Handle HTTP response errors."""
        if response.status == 404:
            error_mapping = (ErrorType.RESPONSE_ERROR, ErrorReason.NOT_FOUND)
        elif response.status >= 500:
            error_mapping = (ErrorType.RESPONSE_ERROR, ErrorReason.SERVER_ERROR)
        elif response.status != 200:
            error_mapping = (ErrorType.RESPONSE_ERROR, ErrorReason.UNEXPECTED_STATUS)
        else:
            return  # No error

        self.handle_error(error_mapping, request_url=response.url, response=response)

    def validate_attribute(self, response, product, attribute_name, css_selector, original_url):
        """Validate the presence of an attribute and log an error if it's missing."""
        value = product.css(css_selector).get()
        if not value:
            error_mapping = (ErrorType.PARSING_ERROR, ErrorReason.MISSING_ATTRIBUTE)
            self.handle_error(
                error_mapping,
                request_url=original_url,
                proxy_url=response.url,
                exception=f"Missing attribute: {attribute_name}"
            )
        return value

    def handle_scraper_error(self, request_url,error_reason: ErrorReason, proxy_url=None,exception=None):
        """Handle scraper configuration errors."""
        error_mapping = (ErrorType.SCRAPER_ERROR, error_reason)
        self.handle_error(error_mapping, request_url=request_url, proxy_url= proxy_url,exception=exception)

    def handle_general_error(self, request_url,exception,proxy_url=None):
        """Log any unspecified or unexpected errors."""
        error_mapping = (ErrorType.GENERAL_ERROR, ErrorReason.UNKNOWN_ERROR)
        self.handle_error(error_mapping, request_url=request_url, proxy_url=proxy_url, exception=exception)

class ScrapySignals:
    successful_requests = 0
    failed_requests = 0
    total_urls = 0
    url_status_map = {}
    scraped_items_count_map = {}
    total_items_on_page = 0
    scraped_items_count = 0
    dropped_items_count = 0
    successful_no_items = []

    @staticmethod
    def connect_signals(crawler, task_id, celery_id):
        signal_map = {
            signals.spider_opened: ScrapySignals.spider_opened,
            signals.spider_closed: ScrapySignals.spider_closed,
            signals.engine_started: ScrapySignals.engine_started,
            signals.engine_stopped: ScrapySignals.engine_stopped,
            signals.request_scheduled: ScrapySignals.request_scheduled,
            signals.request_dropped: ScrapySignals.request_dropped,
            signals.request_reached_downloader: ScrapySignals.request_reached_downloader,
            signals.request_left_downloader: ScrapySignals.request_left_downloader,
            signals.response_received: ScrapySignals.response_received,
            signals.response_downloaded: ScrapySignals.response_downloaded,
            signals.item_scraped: ScrapySignals.item_scraped,
            signals.item_dropped: ScrapySignals.item_dropped,
            signals.spider_idle: ScrapySignals.spider_idle_handler,
            signals.spider_error: ScrapySignals.spider_error,
            scrapy.signals.bytes_received: ScrapySignals.on_bytes_received,
            scrapy.signals.headers_received: ScrapySignals.on_headers_received,
        }
        for signal, handler in signal_map.items():
            crawler.signals.connect(handler, signal=signal)
        ScrapySignals.initialize_csv(task_id, celery_id)

    @staticmethod
    def initialize_csv(task_id, celery_id):
        new_dir = os.path.join(os.getcwd(), 'data')
        os.makedirs(new_dir, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H:%M:%S")
        ScrapySignals.csv_file = os.path.join(new_dir, f'scrapysignals__{task_id}__{celery_id}__{timestamp}.csv')
        with open(ScrapySignals.csv_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['Log Type', 'Message', 'Original URL', 'Proxy URL', 'Status Code', 'Date', 'Time', 'Total Time Taken (seconds)'])
            writer.writeheader()

    @staticmethod
    def log_event(log_type, message, original_url=None, proxy_url=None, status_code=None, total_time_taken=None):
        try:
            current_time = datetime.utcnow()
            date_str = current_time.strftime('%Y-%m-%d')
            time_str = current_time.strftime('%H:%M:%S')
            with open(ScrapySignals.csv_file, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([log_type, message, original_url, proxy_url, status_code, date_str, time_str, total_time_taken])
        except Exception as e:
            logging.error("Failed to write log to CSV: %s", e)

    @staticmethod
    def log_and_update(message, log_type, request=None, status_code=None, status_map_key=None):
        """
        Logs a message and updates the URL status map if required.
        """
        logging.info(message)
        original_url = request.meta.get('original_url') if request else None
        proxy_url = request.meta.get('proxy_url') if request else None
        if status_map_key:
            ScrapySignals.url_status_map[status_map_key] = status_code
        ScrapySignals.log_event(log_type, message, original_url, proxy_url, status_code)

    @staticmethod
    def spider_opened(spider):
        ScrapySignals.start_time = datetime.utcnow()
        # Log spider opened event
        ScrapySignals.log_event(f"{spider.name} opened", "Spider Opened")

    @staticmethod
    def spider_closed(spider, reason):
        total_time_taken = (datetime.utcnow() - ScrapySignals.start_time).total_seconds() if ScrapySignals.start_time else "N/A"
        # Log spider closed event
        ScrapySignals.log_event(f"{spider.name} closed", "Spider Closed", total_time_taken=total_time_taken)

        # Log the status of each URL
        for url, status in ScrapySignals.url_status_map.items():
            ScrapySignals.log_event("URL Status", f"URL: {url} Status: {status}", original_url=url, total_time_taken=total_time_taken)

    @staticmethod
    def engine_started():
        ScrapySignals.log_event("Engine Started", "Engine Started")

    @staticmethod
    def engine_stopped():
        ScrapySignals.log_event("Engine Stopped", "Engine Stopped")

    @staticmethod
    def request_scheduled(request, spider):
        ScrapySignals.total_urls += 1
        ScrapySignals.log_and_update("Request Scheduled", "Request Scheduled", request=request, status_map_key=request.url, status_code="Scheduled")

    @staticmethod
    def request_dropped(request, spider):
        ScrapySignals.failed_requests += 1
        ScrapySignals.log_and_update("Request Dropped", "Request Dropped", request=request, status_map_key=request.url, status_code="Failed")

    @staticmethod
    def request_reached_downloader(request, spider):
        ScrapySignals.log_and_update("Request Reached Downloader", "Request Reached Downloader", request=request)

    @staticmethod
    def request_left_downloader(request, spider):
        ScrapySignals.log_and_update("Request Left Downloader", "Request Left Downloader", request=request)

    @staticmethod
    def response_received(response, request, spider):
        status_code = response.status
        items_scraped = ScrapySignals.scraped_items_count_map.get(request.url, 0)
        if status_code == 200 and items_scraped > 0:
            ScrapySignals.log_and_update("Response received", "Response Received", request=request, status_code=status_code)
        elif status_code == 200 and items_scraped == 0:
            ScrapySignals.successful_no_items.append(request.url)
            ScrapySignals.log_and_update("Response received with no items", "No Items Scraped", request=request, status_code=status_code)
        else:
            ScrapySignals.log_and_update("Failed request", "Failed Request", request=request, status_code=status_code)

    @staticmethod
    def response_downloaded(response, request, spider):
        ScrapySignals.log_and_update("Response Downloaded", "Response Downloaded", request=request)

    @staticmethod
    def item_scraped(item, response, spider):
        request_url = response.request.url
        ScrapySignals.scraped_items_count_map[request_url] = ScrapySignals.scraped_items_count_map.get(request_url, 0) + 1
        ScrapySignals.log_and_update("Item scraped", "Item Scraped", request=response.request)

    @staticmethod
    def item_dropped(item, response, exception, spider):
        ScrapySignals.log_and_update("Item dropped", "Item Dropped", request=response.request)

    @staticmethod
    def spider_idle_handler(spider):
        ScrapySignals.log_and_update("Spider is idle", "Spider Idle")

    @staticmethod
    def spider_error(failure, response, spider):
        ScrapySignals.failed_requests += 1
        ScrapySignals.log_and_update("Spider error", "Spider Error", request=response.request)

    @staticmethod
    def on_bytes_received(data, request, spider):
        ScrapySignals.log_and_update(f"{len(data)} bytes received", "Bytes Received", request=request)

    @staticmethod
    def on_headers_received(headers, request, spider):
        ScrapySignals.log_and_update("Headers Received", "Headers Received", request=request)
