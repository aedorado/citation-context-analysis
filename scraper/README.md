### CITESEERX SCRAPER 

The scraper module scrapes citeseerx and stores data in a sqlite database with schema as shown in the ER diagram.

![scraper-erd](https://cloud.githubusercontent.com/assets/6378532/23202734/5aaf04ec-f905-11e6-8f10-062fe50cb5c6.png)


----
#### Files included are:
1. DB.py : To interact with the database.
2. URL.py : To make requests.
3. log.py : For logging.
4. scrape.py : For scraping.
5. setup.py : For setup.


#### Usage

The requirements for the module are mentioned in the [requirements.txt](https://github.com/aedorado/citation-context-analysis/blob/master/scraper/requirements.txt) file.

    python setup.py [URL]
    python scrape.py

The `URL` mentioned here is the seed URL and must be supplied strictly in the following format:

    http://citeseerx.ist.psu.edu/viewdoc/summary?cid=4875 