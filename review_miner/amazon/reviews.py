import csv
from playwright.sync_api import sync_playwright
import pyotp
from error_manager import ErrorManager
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")
TOTP_SECRET = os.getenv("TOTP_SECRET")
 
# Function to generate the current TOTP code
def get_totp_code(secret):
    totp = pyotp.TOTP(secret)
    return totp.now()
 
url_list = [
    "https://www.amazon.com/product-reviews/B008KJEYLO",
    "https://www.amazon.com/product-reviews/B0B3CPC3KV",
    "https://www.amazon.com/product-reviews/B0B3CPC3K",
 
]
 
def reviewscraper():
    error_manager = ErrorManager(task_id="amazon_scraper_task", scraper_name="AmazonReviewScraper")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
 
        # Prepare CSV file
        csv_file = open('amazon_reviews.csv', mode='w', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Request URL", "Response URL", "Review Title", "Review Author Name", "Review Author URL", "Review Date", "Review Description", "Review Images", "Review Country", "Product Name", "Helpful Votes"])
 
        # Iterate through the list of URLs
        for url in url_list:
            try:
                # Navigate to the page
                page.goto(url)
 
                # Check if redirected to the sign-in page
                if page.url.startswith("https://www.amazon.com/ap/signin"):
                    print("Redirected to the sign-in page. Attempting to log in...")
                    # Log in to Amazon
                    page.fill("input[name='email']", EMAIL)
                    page.click("input#continue")
                    page.fill("input[name='password']", PASSWORD)
                    page.click("input#signInSubmit")
                    page.wait_for_selector("input[name='otpCode']")
                    mfa_code = get_totp_code(TOTP_SECRET)
                    page.fill("input[name='otpCode']", mfa_code)
                    page.click("input#auth-signin-button")
                    page.wait_for_selector("div[data-hook='review']", timeout=60000)
                    print("Login successful!")
 
                # Now that we are logged in, navigate to the review page
                page.goto(url)
                page.wait_for_selector("div[data-hook='review']", timeout=60000)
 
                # Extract data from all pages of reviews
                while True:
                    print(f"Scraping data from {url}...")
                    product_name_element = page.query_selector("span.product-title")
                    product_name = product_name_element.inner_text().strip() if product_name_element else "N/A"
 
                    reviews = page.query_selector_all("div[data-hook='review']")
                    if not reviews:
                        print("No reviews found on the page.")
                        break
 
                    for review in reviews:
                        try:
                            review_title_element = review.query_selector("a.review-title span")
                            review_title = review_title_element.inner_text().strip() if review_title_element else "N/A"
 
                            review_author_element = review.query_selector("span.a-profile-name")
                            review_author_name = review_author_element.inner_text().strip() if review_author_element else "N/A"
                            review_author_url_element = review.query_selector("a.a-profile")
                            review_author_url = review_author_url_element.get_attribute("href") if review_author_url_element else "N/A"
 
                            review_date_element = review.query_selector("span.review-date")
                            review_date = review_date_element.inner_text().strip() if review_date_element else "N/A"
 
                            review_description_element = review.query_selector("span.review-text-content span")
                            review_description = review_description_element.inner_text().strip() if review_description_element else "N/A"
 
                            review_images_elements = review.query_selector_all("img.review-image-tile")
                            review_images = [img.get_attribute("src") for img in review_images_elements] if review_images_elements else []
 
                            review_country = "N/A"  # Amazon reviews often have the country info hidden, it may require additional extraction logic
 
                            helpful_votes_element = review.query_selector("span.cr-vote-text")
                            helpful_votes = helpful_votes_element.inner_text().strip() if helpful_votes_element else "0"
 
                            # Write data to CSV
                            csv_writer.writerow([
                                url,
                                page.url,
                                review_title,
                                review_author_name,
                                review_author_url,
                                review_date,
                                review_description,
                                ", ".join(review_images),
                                review_country,
                                product_name,
                                helpful_votes
                            ])
                        except Exception as e:
                            error_manager.log_error(error_manager.create_error_data(
                                request_url=url,
                                error_name="ReviewProcessingError",
                                error_description="An error occurred while processing a review.",
                                exception=e
                            ))
                            print(f"An error occurred while processing a review: {e}")
 
                    # Check if there is a next page
                    next_page = page.query_selector("li.a-last a")
                    if next_page and "a-disabled" not in (next_page.get_attribute("class") or ""):
                        try:
                            next_page.click()
                            page.wait_for_load_state("domcontentloaded")
                            page.wait_for_selector("div[data-hook='review']", timeout=60000)
                        except Exception as e:
                            error_manager.log_error(error_manager.create_error_data(
                                request_url=url,
                                error_name="NextPageNavigationError",
                                error_description="An error occurred while navigating to the next page.",
                                exception=e
                            ))
                            print(f"An error occurred while navigating to the next page: {e}")
                            break
                    else:
                        break
 
            except Exception as e:
                error_manager.log_error(error_manager.create_error_data(
                    request_url=url,
                    error_name="URLProcessingError",
                    error_description="An error occurred while processing URL.",
                    exception=e
                ))
                print(f"An error occurred while processing URL {url}: {e}")
 
        # Close the CSV file
        csv_file.close()
 
        # Keep the browser open for debugging
        # input("Press Enter to close the browser...")
        print("Scraping completed!")
 
        # Close the browser
        browser.close()
 
if __name__ == "__main__":
    reviewscraper()
 