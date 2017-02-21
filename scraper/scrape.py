from URL import URL
from DB import DB
from bs4 import BeautifulSoup

db = DB()

# url = URL('http://citeseerx.ist.psu.edu/viewdoc/summary?cid=4320')
url = URL('http://citeseerx.ist.psu.edu/viewdoc/summary?cid=5746')
# url = URL('http://citeseerx.ist.psu.edu/showciting?cid=1402726')
# url = URL('http://www.google.com')
url.open()

if (url.redirect_occured()):
    db.insert('link', {'url': url.get_redirect_url()})

html = url.fetch()

# extract abstract
soup = BeautifulSoup(html, "html.parser")
abstract_div = soup.find("div", {"id": "abstract"})
for tag in abstract_div:
    if tag.name == 'p':
        abstract = tag.findAll(text=True)[0]
        
# extract keywords
keywords_div = soup.find("div", {"id": "keywords"})
for tag in keywords_div:
    if tag.name == 'p':
        alist = tag.findAll("a")
        kws = []
        for anc in alist:
            anc.findAll(text=True)
            kws.append(anc.findAll(text=True)[0])
        kws = ';'.join(kws)
        print kws
        break

# add metadata
db.insert('metadata', {
    'doi': url.get_doi(),
    'abstract': abstract,
    'keyphrases': kws
})

