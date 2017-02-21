from URL import URL
from DB import DB
from bs4 import BeautifulSoup

db = DB()

# url = URL('http://citeseerx.ist.psu.edu/viewdoc/summary?cid=4320')
url = URL('http://citeseerx.ist.psu.edu/viewdoc/summary?cid=5746')
# url = URL('http://citeseerx.ist.psu.edu/showciting?cid=1402726')
# url = URL('http://www.google.com')
url.open()

if (not db.exists('link', url.get_doi()) and url.redirect_occured()):
    db.insert('link', {
        'doi': url.get_doi(),
        'url': url.get_redirect_url()
    })
    
if (not db.exists('metadata', url.get_doi())):
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
            break
    # add metadata
    db.insert('metadata', {
        'doi': url.get_doi(),
        'abstract': abstract,
        'keyphrases': kws
    })

# add citations
cit_html = url.get_citations()
soup = BeautifulSoup(cit_html, "html.parser")
trs = soup.findAll('tr', {'class': None, 'id': None})
for tr in trs:
    td = tr.findAll('td')[1]
    a = td.find('a')
    href = a['href']
    if (href.find('viewdoc') >= 0):
        urlt = 'http://citeseerx.ist.psu.edu/viewdoc/summary' + href[href.find('?'):]
        urlt = URL(urlt)
        urlt.open()
        context = tr.find('p', {'class': 'citationContext'}).findAll(text=True)[0]
        db.insert('citations', {
            'doi_f': url.get_doi(),
            'doi_t': urlt.get_doi(),
            'context': context
        })
        db.insert('link', {
            'doi': urlt.get_doi(),
            'url': urlt.get_redirect_url()
        })

db.link_processed(url.get_doi())