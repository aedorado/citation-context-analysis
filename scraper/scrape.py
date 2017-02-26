from URL import URL
from DB import DB
from bs4 import BeautifulSoup

db = DB()

count = 0
while db.count_unpr():
    # url = URL('http://citeseerx.ist.psu.edu/viewdoc/summary?cid=4320')
    count = count + 1
    url = db.get_unpr()
    print url
    url = URL(url)
    url.open()
    db.update_link(url.get_doi(), 2)
    
    if (not db.exists('link', url.get_doi()) and url.redirect_occured()):
        db.insert('link', {
            'doi': url.get_doi(),
            'url': url.get_redirect_url()
        })
        
    if (not db.exists('metadata', url.get_doi())):
        html = url.fetch()
        # extract abstract
        soup = BeautifulSoup(html, "html.parser")
        title = soup.find('h2').findAll(text=True)[0]
        abstract_div = soup.find("div", {"id": "abstract"})
        for tag in abstract_div:
            if tag.name == 'p':
                abstract = tag.findAll(text=True)
                if len(abstract) is not 0:
                    abstract = abstract[0]
                else:
                    abstract = ''
        # extract keywords
        keywords_div = soup.find("div", {"id": "keywords"})
        kws = []
        for tag in keywords_div:
            if tag.name == 'p':
                alist = tag.findAll("a")
                for anc in alist:
                    anc.findAll(text=True)
                    kws.append(anc.findAll(text=True)[0])
                break
        kws = ';'.join(kws)
        # add metadata
        db.insert('metadata', {
            'doi': url.get_doi(),
            'title': title,
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
            print ' -> ', urlt.get_url()
            if (urlt.status_ok()):
                # print tr.find('p', {'class': 'citationContext'})
                if tr.find('p', {'class': 'citationContext'}):
                    context = tr.find('p', {'class': 'citationContext'}).findAll(text=True)[0]
                else:
                    context = ''
                if not db.exists('citations', { 'doi_f': url.get_doi(), 'doi_t': urlt.get_doi()}):
                    db.insert('citations', {
                        'doi_f': url.get_doi(),
                        'doi_t': urlt.get_doi(),
                        'context': context
                    })
                if not db.exists('link', urlt.get_doi()):
                    db.insert('link', {
                        'doi': urlt.get_doi(),
                        'url': urlt.get_url()
                    })
            else:
                print 'ERROR'
    
    db.update_link(url.get_doi(), 1)
else:
    print 'No unprocessed links.'