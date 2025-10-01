#!/usr/bin/env python3
# License: MIT
'''
File: tests.py
Problem Domain: Regression testing
Status: PRODUCTION / STABLE
Revision: 1.0.0
'''

import sys
if '..' not in sys.path:
    sys.path.append('..')

from bible9000.tui import BasicTui
from bible9000.sierra_dao import SierraDAO
from bible9000.sierra_note import NoteDAO
from bible9000.words import WordList
from bible9000.sierra_fav import FavDAO

def test_dao():
    ''' Ye Olde Testing '''
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

def test_favs():
    import os, os.path
    testdb = "~test.sqlt3"
    if os.path.exists(testdb):
        os.unlink(testdb)
    if os.path.exists(testdb):
        raise Exception(f'Unable to remove "{testdb}"?')
    from bible9000.admin_ops import tables
    db = FavDAO.GetDAO(True, testdb)
    db.dao.conn.execute(tables['SqlFav'])
    tests = [
        1, 2, 12, 3000, 3100
        ]

    for t in tests:
        db.toggle_fav(t)        
    for row in db.get_favs():
        if not db.is_fav(row):
            raise Exception("is_fav: - error")

    for t in tests:
        db.toggle_fav(t)
        if db.is_fav(t):
            raise Exception("is_fav: + error")
    for row in db.get_favs():
        print(row)
    # db.dao.conn.connection.rollback()
    db.dao.conn.connection.close()
    if os.path.exists(testdb):
        os.unlink(testdb)

def test_words():
    lines = WordList.Edit(None)
    lines = WordList.Edit('')
    zin = 'able.|$"baker".|$charley.|$delta.|$zulu'
    lines = WordList.Edit(zin)
    print(lines)


def test_notes():
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
        db.insert_or_update_note(row)
    for row in list(db.get_all()):
        cols = row.Notes
        cols[0] = 'Updated ' + cols[0]
        row.Notes = cols
        cols = row.Subject
        cols[0] = 'Updated ' + cols[0]
        row.Subject = cols
        db.update_note(row)
        print('~')
    for row in db.get_all():
        print('ZNOTE',row.__dict__)
    print('SLISTS',db.get_subjects_list())
    # db.dao.conn.connection.rollback()
    db.dao.conn.connection.close()
    if os.path.exists(testdb):
        os.unlink(testdb)


if __name__ == '__main__':
    test_dao()
    test_words()
    test_notes()
    test_favs()
    BasicTui.Display("Testing Success")

