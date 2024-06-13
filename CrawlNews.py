import logging
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException, StaleElementReferenceException
from selenium.webdriver.common.by import By
import pandas as pd
import numpy as np
from time import sleep
import random
from datetime import datetime
import os
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    def install_driver():
        for attempt in range(3):
            try:
                return EdgeService(EdgeChromiumDriverManager().install())
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    sleep(random.uniform(1, 3))
                else:
                    raise e

    # Clean up existing WebDriver cache
    webdriver_cache = os.path.expanduser("~/.wdm/drivers/edgedriver/linux64/")
    if os.path.exists(webdriver_cache):
        shutil.rmtree(webdriver_cache)

    options = Options()
    options.add_argument("--headless")
    options.page_load_strategy = 'normal'
    driver = webdriver.Edge(service=install_driver(), options=options)
    driver.implicitly_wait(10)
    return driver

def is_within_date_range(date_text, start_date, end_date):
    date_format = "%d/%m/%Y %H:%M"  # Adjusted to include the time part
    try:
        date = datetime.strptime(date_text, date_format)
        return start_date <= date <= end_date
    except ValueError:
        logging.warning(f"Date format error: {date_text}")
        return False

def scrape_news_for_stock(driver, stock, start_date, end_date):
    driver.get(f"https://s.cafef.vn/tin-doanh-nghiep/{stock}/event.chn")
    time_list, title_list, link_list, content_list = [], [], [], []
    count = 1

    while True:
        try:
            time_elems = driver.find_elements(By.CSS_SELECTOR, ".timeTitle")
            title_elems = driver.find_elements(By.CSS_SELECTOR, ".docnhanhTitle")

            times = [elem.text for elem in time_elems]
            titles = [elem.text for elem in title_elems]
            links = [elem.get_attribute('href') for elem in title_elems]

            for i in range(len(times)):
                if is_within_date_range(times[i], start_date, end_date):
                    time_list.append(times[i])
                    title_list.append(titles[i])
                    link_list.append(links[i])
                else:
                    logging.info(f"Article {titles[i]} is out of the date range, stopping scrape for this stock.")
                    break

            logging.info(f"Page {count}: Extracted {len(times)} times, {len(titles)} titles, and {len(links)} links")
            if len(times) == 0 or not any(is_within_date_range(time, start_date, end_date) for time in times):
                break

            next_page = driver.find_element(By.XPATH, "/html/body/form/div[3]/div[2]/div[3]/div[3]/div/span[2]")
            next_page.click()
            logging.info("Clicked on button next")
            sleep(random.randint(5, 10))

        except (ElementNotInteractableException, NoSuchElementException) as e:
            logging.info("Reached the last page or element not interactable: %s", e)
            break
        except Exception as e:
            logging.error("Exception occurred: %s", e)
            break

        count += 1

    logging.info(f"Collected {count - 1} pages from {stock} News")

    for link in link_list:
        for attempt in range(3):  # Try to load the page up to 3 times
            try:
                driver.get(link)
                element_present = EC.presence_of_element_located((By.XPATH,
                                                                  "/html/body/form/div[3]/div[2]/div[1]/table/tbody/tr[1]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/table/tbody"))
                WebDriverWait(driver, 30).until(element_present)
                content = driver.find_element(By.XPATH,
                                              "/html/body/form/div[3]/div[2]/div[1]/table/tbody/tr[1]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/table/tbody")
                content_list.append(content.text)
                logging.info(f"Successfully collected content {link}")
                break  # Exit the retry loop if successful
            except (TimeoutException, NoSuchElementException, ElementNotInteractableException) as e:
                logging.warning("Exception on page %s (attempt %d): %s", link, attempt + 1, e)
                if attempt == 2:  # If this was the last attempt, log the error
                    content_list.append("")
                sleep(random.randint(1, 3))

    return pd.DataFrame({
        'Time': time_list,
        'Title': title_list,
        'Link': link_list,
        'Content': content_list
    })

def main():
    start_date = datetime.strptime("01/06/2024", "%d/%m/%Y")
    end_date = datetime.strptime("01/01/2050 00:00", "%d/%m/%Y %H:%M")
    stock = 'HPG'

    driver = setup_driver()
    try:
        logging.info(f"Crawling news for {stock} within date range")
        news = scrape_news_for_stock(driver, stock, start_date, end_date)

        # Read old news data
        try:
            news_old = pd.read_csv(f'/News/{stock}_news.csv', index_col=False)
            # Concatenate new and old data
            combined_news = pd.concat([news, news_old])
            # Remove duplicates
            combined_news = combined_news.drop_duplicates(subset=['Link'])
        except FileNotFoundError:
            logging.warning(f"No existing news file found for {stock}, creating a new one.")
            combined_news = news

        # Save combined data back to CSV
        combined_news.to_csv(f'/News/{stock}_news.csv', index=False,
                             encoding='utf-8-sig')
        logging.info(f"Successfully scraped and merged {stock} news")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
