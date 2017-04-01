from selenium import webdriver
import time, os

# chrome_options = webdriver.ChromeOptions()
# chrome_options.add_argument("--incognito")
# driver = webdriver.Chrome(chrome_options=chrome_options)
driver = webdriver.Chrome()

interests = ['a conceptual architecture for semantic web services 2004']
driver.get("https://academic.microsoft.com/#/search");

try:
    # driver.find_element_by_id("searchControl").click()
    searchControl = driver.find_element_by_class_name("searchControl")
    # for interest in interests:
    searchControl.send_keys(interests[0] + '\n')
    # driver.find_element_by_id("textbtn").click()

    # while True:
    #     time.sleep(11)

    #     chatmsg=driver.find_element_by_class_name("chatmsg")
    #     chatmsg.send_keys("Hello\n")

    #     time.sleep(5)
    #     disconnectbtn=driver.find_element_by_class_name("disconnectbtn")
    #     disconnectbtn.click()
    #     disconnectbtn.click()
    #     disconnectbtn.click()

except:
    driver.close()
    raise