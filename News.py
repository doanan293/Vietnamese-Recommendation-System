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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver():
    options = Options()
    options.add_argument("--headless")  # Ensure headless mode is activated
    options.add_argument("--disable-gpu")  # Disable GPU for headless mode
    driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()), options=options)
    driver.set_page_load_timeout(120)  # Set page load timeout to 120 seconds
    driver.implicitly_wait(10)  # Set implicit wait time to 10 seconds
    return driver

def get_stock_list(driver):
    try:
        driver.get("https://finance.vietstock.vn/chung-khoan-phai-sinh/vn30f1m/hdtl-cp-anh-huong.htm")
        WebDriverWait(driver, 120).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".m-b .text-center [href]"))
        )
        stocks = driver.find_elements(By.CSS_SELECTOR, ".m-b .text-center [href]")
        return [elem.text for elem in stocks]
    except TimeoutException as e:
        logging.error("Timeout while fetching stock list: %s", e)
        return []

def scrape_news_for_stock(driver, stock):
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

            time_list += times
            title_list += titles
            link_list += links

            logging.info(f"Page {count}: Extracted {len(times)} times, {len(titles)} titles, and {len(links)} links")
            if len(times) == 0:
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
                element_present = EC.presence_of_element_located((By.XPATH, "/html/body/form/div[3]/div[2]/div[1]/table/tbody/tr[1]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/table/tbody"))
                WebDriverWait(driver, 30).until(element_present)
                content = driver.find_element(By.XPATH, "/html/body/form/div[3]/div[2]/div[1]/table/tbody/tr[1]/td/table/tbody/tr[2]/td/table/tbody/tr[3]/td/table/tbody")
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
    driver = setup_driver()
    try:
        list_stocks = get_stock_list(driver)
        logging.info("Stock list: %s", list_stocks)
        for stock in list_stocks:
            logging.info(f"Crawling in {stock}")
            news = scrape_news_for_stock(driver, stock)
            news.to_csv(f'D:\\Study Program\\Project\\News\\{stock}_news.csv', index=False, encoding='utf-8-sig')
            logging.info(f"Successfully scraped {stock} news")
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
