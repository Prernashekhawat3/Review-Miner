import scrapy
from datetime import datetime
from amazon.spiders.amz_utils import log_response_info , extract_asin
from proxy import  ProxyManager

# Importing scrapy signals and error messages
from scrapermanagement import ScrapySignals, ErrorManager,ErrorType, ErrorReason
from ..items import AmazonPDPItem

class AmazonPDPSpider(scrapy.Spider):
    name = "amz_pdp"
      
    # Track already scraped product IDs to prevent re-scraping
    scraped_ids = set()

    scraper_API = True

    def __init__(self, urls='', task_id='', celery_id='', *args, **kwargs):
        super(AmazonPDPSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls.split(',') if urls else ['https://www.amazon.com/dp/B093QLTD9Q']
        self.start_urls = urls.split(',')
        self.task_id = task_id
        self.celery_id = celery_id
        self.proxy_manager = ProxyManager()
        self.error_manager = ErrorManager(task_id, celery_id, self.name)
        self.item_cls = AmazonPDPItem()
        
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
                        meta={'original_url': url, 'proxy_url': new_url, 'page_count': 1},
                        dont_filter=True
                    )
                else:
                    # Handle case where ScraperAPI URL generation fails
                    self.error_manager.handle_error(
                        (ErrorType.SCRAPER_ERROR, ErrorReason.INVALID_API_KEY),
                        request_url=url,
                        proxy_url=None
                    )
            except Exception as e:
                self.error_manager.handle_error(
                (ErrorType.SCRAPER_ERROR, ErrorReason.UNKNOWN_ERROR),
                request_url=url,  # Original URL
                proxy_url=new_url if 'new_url' in locals() else None,  # Proxied URL if available
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
                    meta={**failure.request.meta, 'original_url': original_url, 'proxy_url': scrapeops_url},
                    dont_filter=True
                )
            else:
                # Handle case where ScrapeOps URL generation fails
                self.error_manager.handle_error(
                    (ErrorType.SCRAPER_ERROR, ErrorReason.INVALID_API_KEY),
                    request_url=original_url,
                    proxy_url=None,
                    exception="Failed to generate fallback ScrapeOps URL."
                )
        except Exception as e:
            # Handle unknown errors during fallback
            self.error_manager.handle_error(
                (ErrorType.SCRAPER_ERROR, ErrorReason.UNKNOWN_ERROR),
                request_url=original_url,
                proxy_url=None,
                exception=f"Error in fallback_to_scrapeops: {str(e)}"
            )
            
    def handle_network_error(self, failure):
        """Delegate network errors to ErrorManager."""
        self.error_manager.handle_network_error(failure)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AmazonPDPSpider, cls).from_crawler(crawler, *args, **kwargs)
        ScrapySignals.connect_signals(crawler, spider.task_id, spider.celery_id)
        return spider
    
    def parse(self, response):
        try:
            log_response_info(response)
            product_url = response.meta.get('product_url')
            main_product_id = response.meta.get('main_product_id')
            is_variant = response.meta.get('is_variant', False)

            # Handle HTTP response errors
            self.error_manager.handle_response_errors(response)

            if response.status == 200:
                try:

                    product_name = self.error_manager.validate_attribute(response, response, "product_name", "span#productTitle::text", product_url)
                    if not product_name:
                        self.logger.error(f"Skipping product due to missing product name: {product_url}")
                        return

                    price = self.error_manager.validate_attribute(response, response, "price", "span.a-price span.a-offscreen::text", product_url)
                    brand = self.error_manager.validate_attribute(response, response, "brand", "a#bylineInfo::text", product_url)
                    total_rating = self.error_manager.validate_attribute(response, response, "total_rating", "i.a-icon.a-icon-star span.a-icon-alt::text", product_url)

                    variation_product_ids = response.css('li[data-csa-c-item-id]::attr(data-csa-c-item-id)').getall()
                    variation_product_ids = [var_id for var_id in variation_product_ids if var_id != extract_asin(product_url)]

                    titles = response.css('#detailBullets_feature_div .a-text-bold::text').getall()
                    values = response.css('#detailBullets_feature_div .a-text-bold + span::text').getall()
                    titles = [title.strip() for title in titles]
                    values = [value.strip() for value in values]
                    product_details = dict(zip(titles, values))

                    bulletings = response.css('#feature-bullets ul li span::text').getall()
                    bulletings = [bulleting.strip() for bulleting in bulletings if bulleting.strip()]

                    product_description_text = ' '.join(response.css('#productDescription *::text').extract()).strip()

                    product_info = {}
                    rows = response.css('#productOverview_feature_div tr') or response.css('.prodDetTable tr')
                    for row in rows:
                        label = row.css('td:nth-child(1) span::text').get(default='').strip()
                        value = row.css('td:nth-child(2) span::text').get(default='').strip()
                        if label and value:
                            product_info[label] = value

                    item_obj = AmazonPDPItem()
                    item_obj['product_name'] = product_name.strip()
                    item_obj['rating'] = response.css("i.a-icon.a-icon-star span.a-icon-alt::text").get()
                    item_obj['review_count'] = total_rating.strip() if total_rating else None
                    item_obj['image_urls'] = response.css('ul.regularAltImageViewLayout li img::attr(src)').getall()
                    item_obj['num_of_images'] = len(item_obj['image_urls'])
                    item_obj['product_info'] = product_info
                    item_obj['product_details'] = product_details
                    item_obj['bulletings'] = bulletings
                    item_obj['ships_from'] = response.css("#fulfillerInfoFeature_feature_div .offer-display-feature-text-message::text")
                    item_obj['sold_by'] = response.css("#merchantInfoFeature_feature_div .offer-display-feature-text-message::text")
                    item_obj['product_description'] = product_description_text
                    item_obj['price'] = price.strip() if price else None
                    item_obj['brand'] = brand.strip()
                    item_obj['source'] = 'Amazon'
                    item_obj['main_product_id'] = main_product_id
                    item_obj['is_variation'] = is_variant
                    item_obj['variant_image_urls'] = response.css('ul.regularAltImageViewLayout li img::attr(src)').getall()
                    item_obj['variant_product_ids'] = variation_product_ids
                    item_obj['variant_count'] = len(item_obj['variant_product_ids'])
                    item_obj['product_url'] = product_url
                    item_obj['scrapeddate'] = datetime.utcnow().strftime("%Y-%m-%d")
                    item_obj['scrapedtime'] = datetime.utcnow().strftime("%H:%M:%S")
                    item_obj['product_id'] = extract_asin(product_url)

                    yield item_obj

                    for variant_id in variation_product_ids[:3]:
                        if variant_id not in self.scraped_ids:
                            self.scraped_ids.add(variant_id)
                            variant_url = 'https://www.amazon.com/dp/' + variant_id
                            new_url = self.proxy_manager.get_proxy_url(variant_url) if self.scraper_API else variant_url

                            if new_url:
                                yield scrapy.Request(
                                    url=new_url,
                                    callback=self.parse,
                                    errback=self.handle_network_error,
                                    meta={'product_url': variant_url, 'proxy_url': new_url, 'is_variant': True},
                                    dont_filter=True,
                                )
                except Exception as e:
                    self.error_manager.handle_error(
                        (ErrorType.PARSING_ERROR, ErrorReason.UNEXPECTED_STRUCTURE),
                        request_url=product_url,
                        proxy_url=new_url,
                        exception=f"Error processing product: {str(e)}"
                    )

        except Exception as e:
            self.error_manager.handle_error(
                (ErrorType.PARSING_ERROR, ErrorReason.UNKNOWN_ERROR),
                request_url=response.url,
                proxy_url=response.url,  
                exception=f"Error parsing response: {str(e)}"
            )
                    
    
