import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait  # Neuer Import
from selenium.webdriver.support import expected_conditions as EC  # Neuer Import
from selenium.common.exceptions import TimeoutException
from time import sleep
from webdriver_manager.chrome import ChromeDriverManager

def get_selenium_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Headless-Modus
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    return driver

def get_session():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' +
                      'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                      'Chrome/58.0.3029.110 Safari/537.3'
    }
    session = requests.Session()
    session.headers.update(headers)
    return session

def scrape_indeed_jobs(driver, session, search_query):
    search_url = 'https://www.indeed.com/jobs'
    driver.get(search_url)
    
    try:
        # Warte bis das Suchfeld sichtbar ist
        search_box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, 'q'))
        )
    except TimeoutException:
        print("Suchfeld (name='q') wurde nicht gefunden. Überprüfe den Selektor!")
        driver.quit()
        return

    search_box.send_keys(search_query)
    search_box.send_keys(Keys.RETURN)
    
    # Warte, bis Ergebnisse geladen sind
    sleep(5)

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    job_listings = soup.find_all('div', class_='job_seen_organic_click')
    for job in job_listings:
        job_title_element = job.find('h2', class_='jobTitle')
        company_element = job.find('span', class_='company')
        location_element = job.find('div', class_='recJobLoc')
        
        job_title = job_title_element.get_text(strip=True) if job_title_element else 'N/A'
        company_name = company_element.get_text(strip=True) if company_element else 'N/A'
        location = location_element.get('data-rc-loc') if location_element and location_element.has_attr('data-rc-loc') else 'N/A'
        
        print(f'Job Title: {job_title}, Company: {company_name}, Location: {location}')
   
    # Versuche, zur nächsten Seite zu navigieren (wenn vorhanden)
    next_page_link = soup.find('a', class_='nextBtn')
    if next_page_link and next_page_link.get('href'):
        next_page_url = 'https://www.indeed.com' + next_page_link.get('href')
        driver.get(next_page_url)
        sleep(5)
        return scrape_indeed_jobs(driver, session, search_query)
    
    driver.quit()  

def main():
    driver = get_selenium_driver()
    session = get_session()
    search_query = 'Python Developer'
    scrape_indeed_jobs(driver, session, search_query)

if __name__ == '__main__':
    main()