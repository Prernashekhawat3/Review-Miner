import scrapy
from scrapermanagement import ScrapySignals, ErrorManager,ErrorType, ErrorReason
from amazon.spiders.amz_utils import log_response_info
from proxy import  ProxyManager
class AmazonSpider(scrapy.Spider):
    name = "amz_3p"

    def __init__(self, urls='', task_id='', celery_id='', *args, **kwargs):
        super(AmazonSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls.split(',') if urls else ['https://www.amazon.com/dp/B0DCNQM1PY']
        self.task_id = task_id
        self.celery_id = celery_id
        self.proxy_manager = ProxyManager()
        self.error_manager = ErrorManager(task_id, celery_id, self.name)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AmazonSpider, cls).from_crawler(crawler, *args, **kwargs)
        ScrapySignals.connect_signals(crawler, spider.task_id, spider.celery_id)
        return spider
    
    def start_requests(self):
        """
        Use ProxyManager to generate URLs with ScraperAPI and handle errors.
        """
        for url in self.start_urls:
            try:
                # Get ScraperAPI proxied URL
                new_url = self.proxy_manager.get_proxy_url(url, "scraperapi")
                if new_url:
                    self.logger.info(f"Starting request to {new_url} with ScraperAPI")
                    yield scrapy.Request(
                        url=new_url,
                        callback=self.parse,
                        errback=self.handle_network_error,
                        meta={'original_url': url, 'page_count': 1},
                        dont_filter=True
                    )
                else:
                    # Handle case where ScraperAPI URL generation fails
                    self.error_manager.handle_error(
                        (ErrorType.SCRAPER_ERROR, ErrorReason.INVALID_API_KEY),
                        request_url=url
                    )
            except Exception as e:
                # Handle unknown errors during request generation
                self.error_manager.handle_error(
                    (ErrorType.SCRAPER_ERROR, ErrorReason.UNKNOWN_ERROR),
                    request_url=url,
                    exception=f"Error during start_requests: {str(e)}"
                )

    def fallback_to_scrapeops(self, failure):
        """
        Fall back to ScrapeOps proxy if ScraperAPI fails.
        """
        original_url = failure.request.meta['original_url']
        try:
            # Get ScrapeOps proxied URL
            scrapeops_url = self.proxy_manager.get_proxy_url(original_url, "scrapeops")
            if scrapeops_url:
                self.logger.info(f"Falling back to ScrapeOps for {original_url}")
                yield scrapy.Request(
                    url=scrapeops_url,
                    callback=self.parse,
                    errback=self.handle_network_error,
                    meta={**failure.request.meta, 'original_url': original_url},
                    dont_filter=True
                )
            else:
                # Handle case where ScrapeOps URL generation fails
                self.error_manager.handle_error(
                    (ErrorType.SCRAPER_ERROR, ErrorReason.INVALID_API_KEY),
                    request_url=original_url,
                    exception="Failed to generate fallback ScrapeOps URL."
                )
        except Exception as e:
            # Handle unknown errors during fallback
            self.error_manager.handle_error(
                (ErrorType.SCRAPER_ERROR, ErrorReason.UNKNOWN_ERROR),
                request_url=original_url,
                exception=f"Error in fallback_to_scrapeops: {str(e)}"
            )
    def handle_network_error(self, failure):
        """Delegate network errors to ErrorManager."""
        self.error_manager.handle_network_error(failure)

    def parse(self, response):
        log_response_info(response)

        # Handle HTTP response errors
        self.error_manager.handle_response_errors(response)

        if response.status == 200:
            try:
                 # Select the parent div
                offer_section = response.css("#offerDisplayFeatures_desktop")

                if offer_section:
                    data = {
                        "ships_from": offer_section.css(".offer-display-feature-text-message::text").getall()[0].strip(),
                        "sold_by": offer_section.css(".offer-display-feature-text-message::text").getall()[1].strip(),
                        "returns": offer_section.css(".offer-display-feature-text-message::text").getall()[2].strip(),
                        "customer_service": offer_section.css(".offer-display-feature-text-message::text").getall()[3].strip(),
                        "product_id": offer_section.css('#offerDisplayFeatures_desktop::attr(data-csa-c-asin)').get()
                    }
                    yield data
                else:
                    self.logger.error("Offer display section not found on the page.")
            except Exception as e:
                self.error_manager.handle_error(
                    (ErrorType.PARSING_ERROR, ErrorReason.UNKNOWN_ERROR),
                    request_url=response.url,
                    exception=f"Error parsing response: {str(e)}"
                )
