#! /usr/bin/env python3
# Status: Testing Success.
import sqlite3
from bible9000.tui import BasicTui
from bible9000.sierra_dao import SierraDAO

class NoteDAO():
    ''' Manage the NoteDAOs Table '''
    def __init__(self):
        pass

##    def set(self, vStart=0, vEnd=0, kwords='', Subject='', Notes='', NextId=0):
##        self.vStart = vStart
##        self.vEnd   = vEnd
##        self.kwords = kwords
##        self.Subject= Subject
##        self.Notes  = Notes
##        self.NextId = NextId

    def encode(self, text):
        return text.replace('"',"''")
    
    def decode(self, text):
        return text.replace("''",'"')
    
    @staticmethod
    def GetDAO(bSaints=False, database=None):
        ''' Connect to the database & return the DAO '''
        if not database:
            from bible9000.admin_ops import get_database
            database = get_database()
        result = NoteDAO()
        result.dao = SierraDAO.GetDAO(bSaints, database)
        return result

    def insert_note(self, sierra:int, note:str)->bool:
        note = self.encode(note)
        cmd = f'INSERT INTO SqlNotes (vStart, Notes) VALUES ({sierra}, "{note}");'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        return True
    
    def delete_note(self, zid)->bool:
        cmd = f'DELETE from SqlNotes WHERE ID = {zid};'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        return True
        
    def update_note(self, zid, znote)->bool:
        znote = self.encode(znote)
        cmd = f'UPDATE SqlNotes SET Notes="{znote}" WHERE ID = {zid};'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        return True
        
    def notes_for(self, sierra):
        ''' Get all notes on a verse. '''
        cmd = f"SELECT * FROM SqlNotes WHERE vStart = {sierra} ORDER BY vStart;"
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                l = list(a)
                l[5] = self.decode(l[5])
                yield l
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_notes(self):
        ''' Get all notes. '''
        cmd = "SELECT * FROM SqlNotes ORDER BY vStart;"
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                l = list(a)
                l[5] = self.decode(l[5])
                yield l
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None


if __name__ == '__main__':
    import os, os.path
    testdb = "~test.sqlt3"
    if os.path.exists(testdb):
        os.unlink(testdb)
    if os.path.exists(testdb):
        raise Exception(f'Unable to remove "{testdb}"?')
    from bible9000.admin_ops import tables
    db = NoteDAO.GetDAO(True, testdb)
    db.dao.conn.execute(tables['SqlNotes'])
    tests = [
        1, 2, 12, 3000, 3100
        ]
    for t in tests:
        db.insert_note(t, f"x{t}")        
    for row in db.get_notes():
        print(row)
    # db.dao.conn.connection.rollback()
    db.dao.conn.connection.close()
    if os.path.exists(testdb):
        os.unlink(testdb)

