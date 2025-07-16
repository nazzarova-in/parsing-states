import json
import time
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from requests.cookies import RequestsCookieJar


def get_fresh_cookies_and_token(login: str, password: str):
  options = uc.ChromeOptions()

  driver = uc.Chrome(options=options)

  driver.get("https://biz.sosmt.gov/search/business")

  wait = WebDriverWait(driver, 30)
  print("Waiting for the login button...")
  login_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.login-link")))
  login_button.click()

  print("Waiting for the login form...")
  wait.until(EC.visibility_of_element_located((By.ID, "username")))

  print("Enter your login and password...")
  driver.find_element(By.ID, "username").send_keys(login)
  driver.find_element(By.ID, "password").send_keys(password)

  print("Click the login button...")
  driver.find_element(By.CSS_SELECTOR, "button.submit").click()

  time.sleep(7)

  auth_data_raw = driver.execute_script("return window.localStorage.getItem('WEB_USER_AUTHENTICATION');")
  auth_data = json.loads(auth_data_raw)
  authorization_token = auth_data.get("ID")
  print("Authorization token:", authorization_token)


  print("Let's go to the API URL to get the final cookies...")
  driver.get("https://biz.sosmt.gov/api/Records/businesssearch")
  time.sleep(10)

  selenium_cookies = driver.get_cookies()
  print(selenium_cookies)
  driver.quit()

  jar = RequestsCookieJar()
  for cookie in selenium_cookies:
    jar.set(
      name=cookie['name'],
      value=cookie['value'],
      domain=cookie.get('domain'),
      path=cookie.get('path', '/'),
      secure=cookie.get('secure', False),
      rest={'HttpOnly': cookie.get('httpOnly', False)},
      expires=cookie.get('expiry')
    )
    print(jar)

  return jar, authorization_token
