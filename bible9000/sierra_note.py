#!/usr/bin/env python3
'''
File: sierra_note.py
Problem Domain: Database / DAO
Status: PRODUCTION / STABLE
Revision: 1.7.0
'''

import sys
if '..' not in sys.path:
    sys.path.append('..')

import sqlite3
from bible9000.tui import BasicTui
from bible9000.sierra_dao import SierraDAO
from bible9000.words import WordList

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
            if not row[4]:
                self._Subject = ''
            else:
                self._Subject= row[4]
            if not row[5]:
                self._Notes = ''
            else:
                self._Notes  = row[5]
            self.NextId = row[6]

    @property
    def Sierra(self):
        return self.vStart

    @Sierra.setter
    def Sierra(self, value):
        self.vStart = value

    @property
    def Notes(self)->list:
        ''' Always returns a list. '''
        return WordList.StringToList(self.from_db(self._Notes))
    
    @Notes.setter
    def Notes(self, value):
        ''' Assign EITHER a string or a list. '''
        if isinstance(value, str):
            value = [value]
        for ss in range(len(value)):
            value[ss] = self.to_db(value[ss])
        self._Notes = WordList.ListToString(value)

    @property
    def Subject(self)->list:
        ''' Always returns a list. '''
        return WordList.StringToList(self.from_db(self._Subject))
    
    @Subject.setter
    def Subject(self, value):
        ''' Assign EITHER a string or a list. '''
        if isinstance(value, str):
            value = [value]
        for ss in range(len(value)):
            value[ss] = self.to_db(value[ss])
        self._Subject = WordList.ListToString(value)
    
    def add_note(self, note):
        ''' Add a note to the list of '''
        notes = self.Notes
        notes.append(note)
        self.Notes = notes

    def add_subject(self, subject):
        ''' Add a subject to the list of '''
        subjects = self.Subject
        subjects.append(subject)
        self.Subject = subjects
    
    def is_null(self)->bool:
        ''' Bound to change ... '''
        if len(self._Notes) or len(self._Subject):
            return False
        return True

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

    def rollback(self):
        ''' junk recent changes. False if none. '''
        if(self.dao):
            self.dao.conn.connection.rollback()
            return True
        return False
    
    @staticmethod
    def GetDAO(bSaints=False, database=None):
        ''' Connect to the database & return the DAO '''
        if not database:
            from bible9000.admin_ops import get_database
            database = get_database()
        result = NoteDAO()
        result.dao = SierraDAO.GetDAO(bSaints, database)
        return result

    def create_or_insert_note(self, row)->bool:
        ''' Insert or update a note. '''
        if not isinstance(row, NoteDAO):
            return False
        if row.ID:
            return self.update_note(row)
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
        if row.ID == 0:
            return True # ain't there.
        cmd = f'DELETE from SqlNotes WHERE ID = {row.ID};'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        row.ID = 0
        return True
        
    def update_note(self, row)->bool:
        ''' Will also remove any null notes. '''
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
        return True
        
    def note_for(self, sierra):
        ''' Get THE note on a verse. '''
        cmd = f'SELECT * FROM SqlNotes WHERE vStart = {sierra} LIMIT 1;'
        try:
            res = self.dao.conn.execute(cmd)
            if res:
                return NoteDAO(res.fetchone())
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_all(self):
        ''' Get all notes. '''
        cmd = 'SELECT * FROM SqlNotes ORDER BY vStart;'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield NoteDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_notes_only(self):
        ''' Get all notes. '''
        cmd = 'SELECT * FROM SqlNotes \
WHERE Notes <> "" ORDER BY vStart;'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield NoteDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_subjects_only(self):
        ''' Get all notes. '''
        cmd = 'SELECT * FROM SqlNotes \
WHERE Subject <> "" ORDER BY vStart;'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield NoteDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    def get_subjects_list(self)->list:
        ''' Get all Subjects into a sorted list - can be empty. '''
        results = set()
        cmd = 'SELECT * FROM SqlNotes WHERE Subject <> "";'
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                row = NoteDAO(a)
                results = results.union(row.Subject)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return sorted(list(results),reverse=False)


if __name__ == '__main__':
    from tests import test_notes
    test_notes()

