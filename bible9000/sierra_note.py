#!/usr/bin/env python3
'''
File: sierra_note.py
Problem Domain: Database / DAO
Status: PRODUCTION / STABLE
Revision: 1.5.2
'''

import sys
sys.path.append('..')

from bible9000.tui import BasicTui
import sqlite3
from bible9000.tui import BasicTui
from bible9000.sierra_dao import SierraDAO

class NoteDAO():
    ''' Manage the NoteDAOs Table '''
    def __init__(self, row=None):
        if not row:
            self.ID     = 0
            self.vStart = 0
            self.vEnd   = 0
            self.kwords = ''
            self._Subject= ''
            self._Notes  = ''
            self.NextId = 0
        else:
            self.ID     = row[0]
            self.vStart = row[1]
            self.vEnd   = row[2]
            self.kwords = row[3]
            self._Subject= row[4]
            self._Notes  = row[5]
            self.NextId = row[6]


    @property
    def Notes(self):
        return self.from_db(self._Notes)
    
    @Notes.setter
    def Notes(self, value):
        self._Notes = self.to_db(value)

    @property
    def Subject(self):
        return self.from_db(self._Subject)
    
    @Subject.setter
    def Subject(self, value):
        self._Subject = self.to_db(value)

    def to_db(self, text):
        ''' Resore Quotes. '''
        if not text:
            return ''
        text = str(text)
        return text.replace('"',"''")
    
    def from_db(self, text):
        ''' Fix Quotes. '''
        if not text:
            return ''
        text = str(text)
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

    def insert_note(self, row)->bool:
        if not isinstance(row, NoteDAO):
            return False
        cmd = f'INSERT INTO SqlNotes \
(vStart, vEnd, kwords, Subject, Notes, NextId) VALUES \
({row.vStart}, {row.vEnd}, "{row.kwords}", "{row._Subject}", \
"{row._Notes}", {row.NextId});'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        return True
    
    def delete_note(self, row)->bool:
        if not isinstance(row, NoteDAO):
            return False
        cmd = f'DELETE from SqlNotes WHERE ID = {row.ID};'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        return True
        
    def update_note(self, row)->bool:
        if not isinstance(row, NoteDAO):
            return False
        cmd = f'UPDATE SqlNotes SET \
vStart = {row.vStart}, \
vEnd   = {row.vEnd}, \
kwords = "{row.kwords}", \
Subject= "{row._Subject}", \
Notes  = "{row._Notes}", \
NextId = {row.NextId} WHERE ID = {row.ID};'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        print('ok',cmd)
        return True
        
    def notes_for(self, sierra):
        ''' Get all notes on a verse. '''
        cmd = f'SELECT * FROM SqlNotes \
WHERE vStart = {sierra} \
AND Notes <> "" ORDER BY vStart;'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield NoteDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_notes(self):
        ''' Get all notes. '''
        cmd = 'SELECT * FROM SqlNotes WHERE Notes <> "" ORDER BY vStart;'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield NoteDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def subjects_for(self, sierra):
        ''' Get all subject on a verse, else None! '''
        cmd = f'SELECT * FROM SqlNotes \
WHERE vStart = {sierra} \
AND Subject <> "" ORDER BY vStart;'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield NoteDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_subjects(self):
        ''' Get all Subject rows, else None! '''
        cmd = 'SELECT * FROM SqlNotes WHERE Subject <> "" ORDER BY vStart;'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield NoteDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_subjects_list(self)->list:
        ''' Get all Subjects into a sorted list - can be empty. '''
        from words import WordList
        results = set()
        cmd = 'SELECT * FROM SqlNotes WHERE Subject <> "";'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                row = NoteDAO(a)
                results = results.union(WordList.Decode(row.Subject))
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return sorted(list(results),reverse=False)


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
        row = NoteDAO()
        row.vStart  = t
        row.Notes   = f"note{t}"
        row.Subject = f"subject{t}"
        db.insert_note(row)
    for row in list(db.get_notes()):
        row.Notes = f'Updated "{row.Notes}"'
        row.Subject = "Updated " + row.Subject
        db.update_note(row)
        print('~')
    for row in db.get_notes():
        print(row.__dict__)
        print(row.Notes)
    print(db.get_subjects_list())
    # db.dao.conn.connection.rollback()
    db.dao.conn.connection.close()
    if os.path.exists(testdb):
        os.unlink(testdb)

