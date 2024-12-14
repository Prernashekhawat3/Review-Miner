# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonListingItems(scrapy.Item):
    # define the fields for your item here like:
    request_url = scrapy.Field()
    response_url = scrapy.Field()
    product_url = scrapy.Field()
    product_name = scrapy.Field()
    source = scrapy.Field()
    product_id = scrapy.Field()
    source = scrapy.Field()
    node_id = scrapy.Field()
    price = scrapy.Field()
    page_number = scrapy.Field()
    sponsored = scrapy.Field()
    sponsored_brand_name = scrapy.Field()
    sponsored_image_url = scrapy.Field()
    sponsored_product_title = scrapy.Field()
    sponsored_rating = scrapy.Field()
    sponsored_reviews_count = scrapy.Field()
    ratings = scrapy.Field()
    reviewcount = scrapy.Field()
    product_image = scrapy.Field()
    badges = scrapy.Field()
    deals = scrapy.Field()
    type = scrapy.Field()
    ad_id = scrapy.Field()
    product_id = scrapy.Field()
    sponsored = scrapy.Field()
    # sponsored_video_ad_video_url = scrapy.Field()
    # # sponsored_video_ad_poster_image = scrapy.Field()
    # sponsored_video_ad_video_title = scrapy.Field()
    # sponsored_video_ad_product_image = scrapy.Field()
    # sponsored_video_ad_product_price = scrapy.Field()
    # sponsored_video_ad_product_rating = scrapy.Field()
    # sponsored_video_ad_review_count = scrapy.Field()
    # sponsored_video_ad_delivery_info = scrapy.Field()
    # sponsored_brand_ad_brand_logo = scrapy.Field()
    sponsored_brand_name = scrapy.Field()
    sponsored_brand_url = scrapy.Field()
    sponsored_brand_image_url = scrapy.Field()
    # sponsored_brand_ad_brand_video = scrapy.Field()

class AmazonPDPItem(scrapy.Item):
    product_name = scrapy.Field()
    review_count = scrapy.Field()
    rating = scrapy.Field()
    num_of_images = scrapy.Field()
    price = scrapy.Field()
    brand = scrapy.Field()
    ships_from = scrapy.Field()
    sold_by = scrapy.Field()
    bulletings = scrapy.Field()
    product_description = scrapy.Field()
    is_variation = scrapy.Field()
    variant_count = scrapy.Field()  
    variant_image_urls = scrapy.Field()
    product_url = scrapy.Field()
    variant_product_ids = scrapy.Field()
    main_product_id = scrapy.Field()
    image_urls = scrapy.Field()
    product_id = scrapy.Field()
    source = scrapy.Field()
    a_plus_content = scrapy.Field()
    product_details = scrapy.Field()
    product_info = scrapy.Field()
    scrapeddate = scrapy.Field()
    scrapedtime = scrapy.Field()
    
class AmazonReviewItem(scrapy.Item):
    product_name = scrapy.Field()
    rating = scrapy.Field()
    author = scrapy.Field()
    description = scrapy.Field()
    date = scrapy.Field()
    image_url = scrapy.Field()
    product_id = scrapy.Field()
    image_urls = scrapy.Field()
    is_verified = scrapy.Field()
    helpful_votes = scrapy.Field()
    product_url = scrapy.Field()
    
class Amazon3pItem(scrapy.Item):
    asin = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    shipped_from = scrapy.Field()
    sold_by = scrapy.Field()
    
# class AmazonEntityBrowseNodesItem(scrapy.Item):
#     parent_category = scrapy.Field()
#     subcategory_name = scrapy.Field()
#     subcategory_url = scrapy.Field()

# class BestsellerItem(scrapy.Item):
#     category = scrapy.Field()
#     subcategory = scrapy.Field()
#     url = scrapy.Field()

# class BrowseSubcategoryNodeItem(scrapy.Item):
#     category = scrapy.Field()
#     subcategory = scrapy.Field()
#     url = scrapy.Field()

# class SubBrowseNodeItem(scrapy.Item):
#     category = scrapy.Field()
#     subcategory = scrapy.Field()
#     url = scrapy.Field()
