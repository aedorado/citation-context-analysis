import sqlite3 as db

class DB:

    def __init__(self, db_name):
        self.conn = db.connect(db_name)
        self.cursor = self.conn.cursor()
        
    def alter_metadata(self):
        self.cursor.execute('ALTER TABLE metadata ADD COLUMN fieldsofstudy TEXT DEFAULT \'\'')

    def get_all_metadata(self):
        self.cursor.execute('SELECT doi, title FROM metadata WHERE fieldsofstudy = \'\'')
        return self.cursor.fetchall()

    def update_metadata(self, fs, doi):
        # print "UPDATE metadata SET fieldsofstudy = '" + str(fs) + "' WHERE doi = '" + str(doi) + "'"
        self.cursor.execute("UPDATE metadata SET fieldsofstudy = '" + str(fs) + "' WHERE doi = '" + str(doi) + "'")
        self.conn.commit()