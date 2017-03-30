import sys
from DB import DB
from URL import URL

db = DB('citeseerx.db')
db.create_tables()
# db.del_all()

# http://citeseerx.ist.psu.edu/viewdoc/summary?cid=16057
if len(sys.argv) == 2:
    url = URL(sys.argv[1])
    url.open()
    db.insert('link', {
        'doi': url.get_doi(),
        'url': url.get_url()
    })
else:
    print 'Please supply proper URL.'