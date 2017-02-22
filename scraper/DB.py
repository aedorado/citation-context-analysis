import sqlite3 as db

class DB:

    def __init__(self):
        self.conn = db.connect('citeseerx.db')
        self.cursor = self.conn.cursor()
        
    def create_tables(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS link(doi varchar(64)  primary key, url varchar(256), processed boolean)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS metadata(doi VARCHAR(64) primary key, abstract text, keyphrases text)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS citations(doi_f VARCHAR(64), doi_t VARCHAR(64), context TEXT, PRIMARY KEY(doi_f, doi_t))')

    def insert(self, tablename, data):
        if (tablename == 'link'):
            query = 'INSERT INTO link VALUES (?, ?, 0)'
            self.cursor.execute(query, (data['doi'], data['url']))
            self.conn.commit()
        elif (tablename == 'metadata'):
            query = 'INSERT INTO metadata VALUES (?, ?, ?)'
            self.cursor.execute(query, (data['doi'], data['abstract'], data['keyphrases']))
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
            # query = 'INSERT INTO citations VALUES (?, ?, ?)'
            # self.cursor.execute(query, (data['doi_f'], data['doi_t'], data['context']))
            # self.conn.commit()
            pass
    
    def link_processed(self, doi):
        query = 'UPDATE link SET processed = 1 WHERE doi = ?'
        self.cursor.execute(query, (doi, ))
        self.conn.commit()
        
    def count_unpr(self):
        query = 'SELECT COUNT(*) FROM link'
        self.cursor.execute(query)
        return self.cursor.fetchall()[0][0]
    
    def get_unpr(self):
        query = 'SELECT * FROM link where processed = 0'
        self.cursor.execute(query)
        return self.cursor.fetchall()[0][1]
        
    def del_all(self):
        self.cursor.execute('DELETE FROM link;')
        self.cursor.execute('DELETE FROM citations;')
        self.cursor.execute('DELETE FROM metadata;')
        self.conn.commit()
        