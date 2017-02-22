### CITESEERX SCRAPER 

The scraper module scrapes citeseerx and stores data in a sqlite database with schema as shown in the ER diagram.

![erdplus-diagram](https://cloud.githubusercontent.com/assets/6378532/23210048/deb6fdaa-f921-11e6-880c-c5998bfd7773.png)


----
#### Files included are:
1. DB.py : To interact with the database.
2. URL.py : To make requests.
3. log.py : For logging.
4. scrape.py : For scraping.
5. setup.py : For setup.


#### Usage

The requirements for the module are mentioned in the [requirements.txt](https://github.com/aedorado/citation-context-analysis/blob/master/scraper/requirements.txt) file.

    python setup.py URL
    python scrape.py

The `URL` mentioned here is the seed URL and must be supplied strictly in the following format:

    http://citeseerx.ist.psu.edu/viewdoc/summary?cid=4875 
    
To export data to a CSV file from `citeseerx.db`

     sqlite3 -header -csv citeseerx.db "select * from metadata;" > metadata.csv
     sqlite3 -header -csv citeseerx.db "select * from citations;" > citations.csv
     sqlite3 -header -csv citeseerx.db "select * from link;" > link.csv