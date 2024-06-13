from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, ElementNotInteractableException
from selenium.webdriver.common.by import By
import pandas as pd
import random
from time import sleep
import os
import shutil

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
def get_stock_list(driver, url):
    driver.get(url)
    sleep(random.uniform(1, 3))
    stocks = driver.find_elements(By.CSS_SELECTOR, ".m-b .text-center [href]")
    list_stocks = [elem.text for elem in stocks]
    links = [elem.get_attribute('href') for elem in stocks]
    return list_stocks, links

def wait_for_element(driver, xpath, timeout=120):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))

def scrape_data(driver, link):
    driver.get(link)
    data = {}
    while True:
        for m in range(1, 4):
            n = 1
            while True:
                try:
                    wait_for_element(driver, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]")
                    check = driver.find_element(By.XPATH, f"/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]/div[{m}]/table/tbody/tr[{n}]/td[1]")
                    at = check.text
                    if len(at) == 0:
                        break
                    if at not in data:
                        data[at] = []
                except NoSuchElementException:
                    break

                n += 1
        try:
            wait_for_element(driver, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[3]/div[2]/div[2]")
            prev_part = driver.find_element(By.XPATH, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[3]/div[2]/div[2]")
            if 'disabled' in prev_part.get_attribute('class'):
                print("Reached the last page")
                break
            prev_part.click()
            print("Clicked on the 'prev' button")
            sleep(random.uniform(1, 3))
        except Exception as e:
            print(f"Reached the last page or element not interactable: {e}")
            break
    driver.get(link)
    while True:
        for q in range(1, 4):
            j = 1
            while True:
                try:
                    wait_for_element(driver, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]")

                    check = driver.find_element(By.XPATH, f"/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]/div[{q}]/table/tbody/tr[{j}]/td[1]")
                    at = check.text
                    if len(at) == 0:
                        print(f"table {q}")
                        break
                except NoSuchElementException:
                    print("No such element")
                    break

                for k in range(5, 1, -1):
                    try:
                        wait_for_element(driver, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]")
                        value = driver.find_element(By.XPATH, f"/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]/div[{q}]/table/tbody/tr[{j}]/td[{k}]")
                        data[at].append(value.text)
                    except NoSuchElementException:
                        data[at].append('')

                j += 1
        max_length = max(len(v) for v in data.values())
        # Pad each list with None values to make their lengths equal
        for key, value in data.items():
            if len(value) < max_length:
                value.extend([""] * (max_length - len(value)))
        try:
            wait_for_element(driver, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[3]/div[2]/div[2]")
            prev_part = driver.find_element(By.XPATH, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[3]/div[2]/div[2]")
            if 'disabled' in prev_part.get_attribute('class'):
                print("Reached the last page")
                break
            prev_part.click()
            print("Clicked on the 'prev' button")
            sleep(random.uniform(1, 3))
        except Exception as e:
            print(f"Reached the last page or element not interactable: {e}")
            break
    driver.get(link)
    data["Time"] = []
    while True:
        for i in range(5, 1, -1):
            try:
                wait_for_element(driver, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]/div[1]/table/thead/tr")
                quarter = driver.find_element(By.XPATH, f"/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[2]/div[1]/table/thead/tr/th[{i}]")
                data["Time"].append(quarter.text)
            except NoSuchElementException:
                print(f"Reached the last page")
                break
        try:
            wait_for_element(driver, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[3]/div[2]/div[2]")
            prev_part = driver.find_element(By.XPATH, "/html/body/div[4]/div[16]/div/div[5]/div[2]/div[2]/div[1]/div[4]/div/div/div[3]/div[2]/div[2]")
            if 'disabled' in prev_part.get_attribute('class'):
                print("Reached the last page")
                break
            prev_part.click()
            print("Clicked on the 'prev' button")
            sleep(random.uniform(1, 3))
        except (ElementNotInteractableException, NoSuchElementException) as e:
            print(f"Reached the last page")
            break
    max_length = max(len(v) for v in data.values())
    # Pad each list with None values to make their lengths equal
    for key, value in data.items():
        if len(value) < max_length:
            value.extend([""] * (max_length - len(value)))
    return data

def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding='utf-8-sig')

def main():
    driver = setup_driver()
    try:
        #url = "https://finance.vietstock.vn/chung-khoan-phai-sinh/vn30f1m/hdtl-cp-anh-huong.htm"
        #list_stocks, links = get_stock_list(driver, url)
        #print(list_stocks)
        #print(links)
        i = 'HPG'
        links = ['https://finance.vietstock.vn/HPG-hoa-phat-group-jsc.htm']
        for link in links:
            print(f'Crawling {i}')
            retries = 3
            for attempt in range(retries):
                try:
                    data = scrape_data(driver, link)
                    filename = f"/Quarter_report/{i}_quarter_report_new.csv"
                    save_to_csv(data, filename)
                    print(f"Successfully saved {i}")
                    break
                except TimeoutException as e:
                    print(f"Timeout while scraping {i} on attempt {attempt + 1} of {retries}: {e}")
                    if attempt < retries - 1:
                        print("Retrying...")
                        sleep(random.uniform(1, 3))
                    else:
                        print(f"Failed to scrape {i} after {retries} attempts")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
