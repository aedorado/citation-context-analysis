from DB import DB

db = DB('citeseerx.db') # destination
db1 = DB('citeseerx.1.db') # source

links = db1.get_all('link')
citations = db1.get_all('citations')
metadata = db1.get_all('metadata')

for link in links:
    query = "SELECT COUNT(*) FROM link WHERE doi = '" + link[0] + "'"
    db.cursor.execute(query)
    count = db.cursor.fetchall()[0][0]
    if count is 0:
        query = 'INSERT INTO link VALUES (?, ?, ?)'
        db.cursor.execute(query, (link[0], link[1], link[2]))
        db.conn.commit()
        
for data in metadata:
    query = "SELECT COUNT(*) FROM metadata WHERE doi = '" + data[0] + "'"
    db.cursor.execute(query)
    count = db.cursor.fetchall()[0][0]
    if count is 0:
        query = 'INSERT INTO metadata VALUES (?, ?, ?, ?)'
        db.cursor.execute(query, (data[0], data[1], data[2], data[3]))
        db.conn.commit()
        
for cit in citations:
    query = "SELECT COUNT(*) FROM citations WHERE doi_f = '" + cit[0] + "' AND doi_t = '" + cit[1] + "'"
    db.cursor.execute(query)
    count = db.cursor.fetchall()[0][0]
    if count is 0:
        query = 'INSERT INTO citations VALUES (?, ?, ?)'
        db.cursor.execute(query, (cit[0], cit[1], cit[2]))
        db.conn.commit()        