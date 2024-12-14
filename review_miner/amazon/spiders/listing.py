import scrapy
from ..items import AmazonListingItems
from scrapermanagement import ScrapySignals, ErrorManager,ErrorType, ErrorReason
from amazon.spiders.amz_utils import log_response_info ,detect_page_template
from proxy import  ProxyManager
class AmazonSpider(scrapy.Spider):
    name = "amz_listings"

    def __init__(self, urls='', task_id='', celery_id='', *args, **kwargs):
        super(AmazonSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls.split(',') if urls else ['https://www.amazon.com/b?node=196609011']
        self.task_id = task_id
        self.celery_id = celery_id
        self.proxy_manager = ProxyManager()
        self.error_manager = ErrorManager(task_id, celery_id, self.name)
        self.item_cls = AmazonListingItems()

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
                # Handle unknown errors during request generation
                self.error_manager.handle_error(
                    (ErrorType.SCRAPER_ERROR, ErrorReason.UNKNOWN_ERROR),
                    request_url=url,
                    proxy_url=None,
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
                    meta={**failure.request.meta, 'proxy_url': scrapeops_url},
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

    def parse(self, response):
        log_response_info(response)
        original_url = response.meta.get('original_url')
        proxy_url = response.meta.get('proxy_url', response.url)
        page_count = response.meta.get('page_count', 1)

        # Handle HTTP response errors
        self.error_manager.handle_response_errors(response)

        if response.status == 200:
            try:
                # Sponsored Ads
                ads_data = []

                # Locate brand ads using static and reliable class structures
                brand_ads = response.css('[id*="CardInstance"]')
                for ad in brand_ads[:10]:
                    try:
                        brand_name = ad.css("a img::attr(alt)").get()
                        logo_image = ad.css("a img::attr(src)").get()
                        brand_message = ad.css("a span.a-truncate-full::text").get()
                        brand_store_url = ad.css("a::attr(href)").get()
                        main_image_url = ad.css("a img::attr(src)").get()

                        # Extract product details within the brand ad
                        products = []
                        product_elements = ad.css("[data-asin]")
                        for product in product_elements:
                            try:
                                product_name = product.xpath("string(.)").get()
                                product_url = product.css("a.a-link-normal::attr(href)").get()
                                product_image = product.css("img::attr(src)").get()
                                product_id = product.attrib.get('data-asin')
                                rating = product.css("span.a-icon-alt::text").get()
                                reviews = product.css('[data-rt]::text').get()

                                products.append({
                                    "product_name": product_name.strip() if product_name else None,
                                    "product_url": response.urljoin(product_url) if product_url else None,
                                    "product_image": product_image,
                                    "product_id": product_id,
                                    "rating": rating,
                                    "reviews": reviews,
                                })
                            except Exception as e:
                                self.error_manager.handle_error(
                                    (ErrorType.PARSING_ERROR, ErrorReason.UNEXPECTED_STRUCTURE),
                                    request_url=response.url,
                                    proxy_url=proxy_url,
                                    exception=f"Error parsing product details in brand ad: {str(e)}"
                                )

                        ads_data.append({
                            "type": "sponsored_brand",
                            "content": {
                                "brand_name": brand_name,
                                "logo_image": logo_image,
                                "brand_message": brand_message,
                                "brand_store_url": response.urljoin(brand_store_url) if brand_store_url else None,
                                "main_image_url": main_image_url,
                                "products": products,
                            },
                        })
                    except Exception as e:
                        self.error_manager.handle_error(
                            (ErrorType.PARSING_ERROR, ErrorReason.MISSING_ATTRIBUTE),
                            request_url=response.url,
                            proxy_url=proxy_url,
                            exception=f"Error processing sponsored brand ad: {str(e)}"
                        )

                # Sponsored Videos
                sponsored_brand_videos = response.css("span.sbv-video-single-product")
                for video in sponsored_brand_videos:
                    try:
                        title = video.css("h2.a-size-mini.a-spacing-none.a-color-base.s-line-clamp-3 a span::text").get()
                        reviews = video.css("span.a-size-base.s-underline-text::text").get()
                        ratings = video.css("i.a-icon-star-small span.a-icon-alt::text").get()
                        price = video.css("span.a-price span.a-offscreen::text").get()
                        image_url = video.css("img.s-image::attr(src)").get()
                        product_url = video.css("a.a-link-normal::attr(href)").get()
                        video_url = video.css("video::attr(src)").get()
                    
                        ads_data.append({
                            "type": "sponsored_video",
                            "content": {
                                "title": title.strip() if title else None,
                                "reviews": reviews,
                                "ratings": ratings,
                                "price": price,
                                "image_url": image_url,
                                "product_url": response.urljoin(product_url) if product_url else None,
                                "video_url": video_url,
                            }
                        })
                    except Exception as e:
                        self.error_manager.handle_error(
                            (ErrorType.PARSING_ERROR, ErrorReason.UNEXPECTED_STRUCTURE),
                            request_url=response.url,
                            proxy_url=proxy_url,
                            exception=f"Error parsing sponsored video ad: {str(e)}"
                        )

                # Yield sponsored ad data
                for ad in ads_data:
                    yield ad

                # Detect the page template type and get the list of products
                template_type, product_list = detect_page_template(response, self.logger, page_count)

                if not product_list:
                    self.error_manager.handle_error(
                        (ErrorType.RESPONSE_ERROR, ErrorReason.INVALID_RESPONSE),
                        request_url=response.url,
                        exception="No products detected on the page."
                    )
                    return
                
                product_list = response.css('div[data-asin]')
                for product in product_list:
                    try:
                        is_sponsored = bool(product.css('div.a-row.a-spacing-micro'))

                        # Extract the product URL specific to the product element
                        product_url = product.css("h2.a-size-mini a.a-link-normal::attr(href)").get()
                        if not product_url:
                            continue

                        product_url = f"https://www.amazon.com{product_url}"
                        product_name = self.error_manager.validate_attribute(response, product, "product_name", ".a-color-base.a-text-normal::text", original_url)
                        price = self.error_manager.validate_attribute(response, product, "price", ".a-price-whole::text", original_url)
                        ratings = product.css('span.a-icon-alt::text').get()
                        review_count = product.css('.a-size-base.s-underline-text::text').get()
                        product_image = self.error_manager.validate_attribute(response, product, "product_image", ".s-image::attr(src)", original_url)

                        yield {
                            "type": "sponsored_product" if is_sponsored else "organic_product",
                            "request_url": original_url,
                            "response_url": response.url,
                            "product_name": product_name,
                            "product_url": product_url,
                            "source": "Amazon",
                            "price": price,
                            "page_number": page_count,
                            "ratings": ratings,
                            "reviewcount": review_count,
                            "product_image": product_image,
                            "badges": product.css('.a-badge-region .a-badge-text::text').get(),
                            "deals": product.css('.a-row .s-link-style .a-badge-text::text').get(),
                            "sponsored": is_sponsored
                        }
                    except Exception as e:
                        self.error_manager.handle_error(
                            (ErrorType.PARSING_ERROR, ErrorReason.UNEXPECTED_STRUCTURE),
                            request_url=response.url,
                            proxy_url=proxy_url,
                            exception=f"Error processing product: {str(e)}"
                        )

                # Handle pagination
                next_page_url = response.css("a.s-pagination-item.s-pagination-button::attr(href)").get()
                if next_page_url and page_count < 5:  # Limit scraping to 5 pages
                    # Construct the next page URL with Amazon domain
                    next_page_url = f"https://www.amazon.com{next_page_url}"
                    
                    # Use the proxy manager to generate a proxy URL
                    proxied_url = self.proxy_manager.get_proxy_url(next_page_url, "scraperapi")

                    if proxied_url:
                        yield response.follow(
                            proxied_url,
                            callback=self.parse,
                            meta={
                                "original_url": next_page_url,
                                "proxy_url": proxied_url,
                                "page_count": page_count + 1,
                            }
                        )
                    else:
                        self.error_manager.handle_error(
                            (ErrorType.SCRAPER_ERROR, ErrorReason.INVALID_API_KEY),
                            request_url=next_page_url,
                            proxy_url=None,
                            exception="Failed to generate proxy URL for next page."
                        )
            except Exception as e:
                        self.error_manager.handle_error(
                            (ErrorType.PARSING_ERROR, ErrorReason.UNEXPECTED_STRUCTURE),
                            request_url=response.url,
                            proxy_url=proxy_url,
                            exception=f"Error processing product: {str(e)}"
                        )

