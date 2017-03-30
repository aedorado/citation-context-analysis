import sqlite3 as db

class DB:

    def __init__(self, db_name):
        self.conn = db.connect(db_name)
        self.cursor = self.conn.cursor()
        
    def create_tables(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS link(doi varchar(64)  primary key, url varchar(256), processed boolean)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS metadata(doi VARCHAR(64) primary key, title text, abstract text, keyphrases text)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS citations(doi_f VARCHAR(64), doi_t VARCHAR(64), context TEXT, PRIMARY KEY(doi_f, doi_t))')

    def insert(self, tablename, data):
        if (tablename == 'link'):
            query = 'INSERT INTO link VALUES (?, ?, 0)'
            self.cursor.execute(query, (data['doi'], data['url']))
            self.conn.commit()
        elif (tablename == 'metadata'):
            query = 'INSERT INTO metadata VALUES (?, ?, ?, ?)'
            self.cursor.execute(query, (data['doi'], data['title'], data['abstract'], data['keyphrases']))
            self.conn.commit()
        elif (tablename == 'citations'):
            query = 'INSERT INTO citations VALUES (?, ?, ?)'
            self.cursor.execute(query, (data['doi_f'], data['doi_t'], data['context']))
            self.conn.commit()
            
    def exists(self, tablename, key):
        if (tablename == 'link'):
            query = 'SELECT doi FROM link WHERE doi=?'
            self.cursor.execute(query, (key, ))
            allrows = self.cursor.fetchall()
            return (len(allrows) == 1)
        elif (tablename == 'metadata'):
            query = 'SELECT doi FROM metadata WHERE doi=?'
            self.cursor.execute(query, (key, ))
            allrows = self.cursor.fetchall()
            return (len(allrows) == 1)
        elif (tablename == 'citations'):
            query = 'SELECT COUNT(*) FROM citations WHERE doi_f = ? AND doi_t = ?'
            self.cursor.execute(query, (key['doi_f'], key['doi_t']))
            count = self.cursor.fetchall()[0][0]
            return (count == 1)
    
    def update_link(self, doi, status):
        query = 'UPDATE link SET processed = ? WHERE doi = ?'
        self.cursor.execute(query, (status, doi, ))
        self.conn.commit()
        
    def count_unpr(self):
        query = 'SELECT COUNT(*) FROM link WHERE processed = 0'
        self.cursor.execute(query)
        return self.cursor.fetchall()[0][0]
    
    def get_unpr(self):
        query = 'SELECT * FROM link where processed = 0 LIMIT 0,1'
        self.cursor.execute(query)
        return self.cursor.fetchall()[0][1]
        
    def del_all(self):
        self.cursor.execute('DELETE FROM link;')
        self.cursor.execute('DELETE FROM citations;')
        self.cursor.execute('DELETE FROM metadata;')
        self.conn.commit()
        
    def select_all(self, table):
        self.cursor.execute('SELECT * FROM ' + table)
        return self.cursor.fetchall()
    
    def get_all(self, table):
        self.cursor.execute('SELECT * FROM ' + table)
        return self.cursor.fetchall()
    
    def table_to_star_sep(self, table):
        allrows = self.get_all(table)
        f = open(table + '.txt', 'w')
        for row in allrows[1:3]:
            for i in range(0, len(row)):
                if (i == (len(row) - 1)):
                    f.write(row[i].encode('utf-8'))
                else:
                    f.write(row[i].encode('utf-8') + ' *** ')
            f.write('\n')
        f.close()
        print 'File created.'
        