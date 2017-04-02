from selenium import webdriver
import time, os
from DB import DB

os.system('PATH=$PATH:.')

ZERO = 0
driver = webdriver.Chrome()

count = 0

db = DB('citeseerx.1.db')
paper_names = db.get_all_metadata()
# print paper_names
driver.get("https://academic.microsoft.com/#/search");

searchControl = driver.find_element_by_class_name("searchControl")
for paper in paper_names:
    try:
        paper_doi = paper[0]
        paper_title = paper[1]
        print paper_doi
        searchControl.clear()
        searchControl.send_keys(paper_title + '\n')
        time.sleep(5)
        searchResultItem = driver.find_elements_by_class_name("search-result-item")
        if len(searchResultItem) is not 0:
            js = "$('ul#searchDropdown').remove()"
            driver.execute_script(js)
            blueTitle = driver.find_element_by_class_name('blue-title').click()
            time.sleep(5)
            fieldsOfStudyDiv = driver.find_elements_by_class_name('entity-section')[0]
            fieldsOfStudy = fieldsOfStudyDiv.text
            if fieldsOfStudy.find('Study:') is not -1:
                fieldsOfStudy = fieldsOfStudy[fieldsOfStudy.find('Study:') + 6:].strip()
                print fieldsOfStudy
                db.update_metadata(fieldsOfStudy, paper_doi)
                ++count
            else:
                # update with 0
                db.update_metadata(ZERO, paper_doi)
                pass
        else:
            # update with 0
            db.update_metadata(ZERO, paper_doi)
            pass
    except:
        print 'An exception occured for the doi : ' + paper_doi
        # driver.close()
