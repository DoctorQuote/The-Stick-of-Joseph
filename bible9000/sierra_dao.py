#!/usr/bin/env python3
'''
File: sierra_dao.py
Problem Domain: Database / DAO
Status: PRODUCTION / STABLE
Revision: 1.5.1
'''
import sys
import sqlite3
from bible9000.tui import BasicTui


class SierraDAO:
    ''' Extract a nominal PROBLEM DOMAIN dictionary,
        from the database. Partial S3D2 pattern.'''
    
    def __init__(self, cursor, bSaints=False):
        self.conn = cursor
        self.bSaints = bSaints
        self.sql_sel = "SELECT Verse, Book, BookChapterID, BookVerseID, V.ID, BookID \
FROM SqlTblVerse AS V JOIN SqlBooks as B WHERE (B.ID=BookID) AND {zmatch} ORDER BY V.ID;"

    def source(self):
        result = {
            "sierra":None,
            "book":None,
            "chapter":None,
            "verse":None,
            "text":None,
           }
        return result

    def classic2sierra(self, book:str, chapt, verse)->int:
        ''' Convert a classic scripture reference to
            the Sierra Bible number. (primary key.)
            Returns None on error.
        '''
        # BasicTui.Display([book, chapt, verse])
        cmd = f"SELECT V.ID FROM SqlTblVerse AS V JOIN SqlBooks as B \
WHERE (B.ID=BookID) AND BOOK LIKE '%{book}%' AND BookChapterID='{chapt}' AND BookVerseID='{verse}' LIMIT 1;"
        # BasicTui.Display(cmd)
        res = self.conn.execute(cmd)
        try:
            zrow = res.fetchone()
            # BasicTui.Display(zrow)
            if zrow:
                return zrow[0]
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

            
    def search_verse(self, sierra_num):
        ''' Lookup a single sierra verse number.'''
        for result in self.search(f"V.ID={sierra_num}"):
            yield result

    def search_books(self):
        ''' Locate the book inventory - Name of book, only '''
        cmd = "SELECT Book FROM SqlBooks ORDER BY ID;"
        res = self.conn.execute(cmd)
        response = self.source()
        try:
            zrow = res.fetchone()
            while zrow:
                response['book'] = zrow[0]
                if zrow[0].find('.') != -1:
                    cols = zrow[0].split('.')
                    if self.bSaints == False and cols[1] != 'nt' and cols[1] != 'ot':
                        zrow = res.fetchone()
                        continue
                    response['book'] = cols[2]
                yield response
                zrow = res.fetchone()
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None
    
    def search(self, where_clause):
        ''' Search using a LIKE-match - one or many. '''
        cmd = self.sql_sel.replace('{zmatch}', where_clause)
        res = self.conn.execute(cmd)
        response = self.source()
        try:
            zrow = res.fetchone()
            while zrow:
                response['sierra'] = str(zrow[4])
                response['book'] = zrow[1]
                if zrow[1].find('.') != -1:
                    cols = zrow[1].split('.')
                    if self.bSaints == False and cols[1] != 'nt' and cols[1] != 'ot':
                        zrow = res.fetchone()
                        continue
                    response['book'] = cols[2]
                response['chapter'] = zrow[2]
                response['verse'] = zrow[3]
                response['text'] = zrow[0]
                yield response
                zrow = res.fetchone()
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None

    @staticmethod
    def GetNotes(sierra:int):
        ''' get notes for verse '''
        from bible9000.sierra_notes import SqlNote
        dao = GetDAO(True)
        for row in dao.search_notes(sierra):
            yield row
   
    @staticmethod
    def GetDAO(bSaints=False, database=None):
        ''' Connect to the database & return the DAO '''
        if not database:
            from bible9000.admin_ops import get_database
            database = get_database()
        conn = sqlite3.connect(database)
        # conn.row_factory = dict_factory
        curs = conn.cursor()
        dao = SierraDAO(curs, bSaints)
        dao.database = database
        return dao

    
    @staticmethod
    def ListBooks(bSaints=False) -> list():
        ''' Get the major books. Emply list on error. '''
        results = list()
        dao = SierraDAO.GetDAO(bSaints)
        if not dao:
            return results
        books = dao.search_books()
        if not books:
            return results
        return books

    @staticmethod
    def GetBookRange(book_id:int, bSaints=True)->tuple:
        ''' Get the minimum and maximum sierra
            number for the book #, else None
        '''
        try:
            dao = SierraDAO.GetDAO(bSaints)
            cmd = f'select min(id), max(id) from SqlTblVerse where BookID = {book_id};'
            result = dao.conn.execute(cmd)
            return tuple(result.fetchone())
        except Exception as ex:
            BasicTui.DisplayError(ex)
            return None
                
if __name__ == "__main__":
    ''' Ye Olde Testing '''
    from bible9000.tui import BasicTui
    rows = SierraDAO.ListBooks(True)
    if len(list(rows)) != 81:
        BasicTui.DisplayError("Testing Failure - No Books?")
        quit()

    dao = SierraDAO.GetDAO()
    rows = dao.search("verse LIKE '%PERFECT%'")
    if len(list(rows)) != 124:
        BasicTui.DisplayError("Testing Failure")
    else:
        BasicTui.Display("Testing Success")        
