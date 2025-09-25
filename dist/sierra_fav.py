#!/usr/bin/env python3
# Status: Testing Success.
import sqlite3
from bible9000.tui import BasicTui
from bible9000.sierra_dao import SierraDAO

class FavDAO():
    ''' Manage the Fav Table '''
    def __init__(self):
        pass

    @staticmethod
    def GetDAO(bSaints=False, database=None):
        ''' Connect to the database & return the DAO '''
        if not database:
            from bible9000.admin_ops import get_database
            database = get_database()
        result = FavDAO()
        result.dao = SierraDAO.GetDAO(bSaints, database)
        return result
    
    def toggle_fav(self, sierra:int)->bool:
        if self.is_fav(sierra):
            cmd = f'DELETE From SqlFav WHERE item = {sierra};'
        else:
            cmd = f'INSERT INTO SqlFav (item) VALUES ({sierra});'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        return True
    
    def is_fav(self, sierra:int)->bool:
        cmd = f'SELECT * from SqlFav WHERE item = {sierra};'
        res = self.dao.conn.execute(cmd)
        for _ in res:
            return True
        return False
        
    def get_favs(self):
        ''' Get all favorites. '''
        cmd = f"SELECT * FROM SqlFav ORDER BY item;"
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield a
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
    db = FavDAO.GetDAO(True, testdb)
    db.dao.conn.execute(tables['SqlFav'])
    tests = [
        1, 2, 12, 3000, 3100
        ]
    print('toggle on')
    for t in tests:
        db.toggle_fav(t)        
    for row in db.get_favs():
        print(row)
        if not db.is_fav(row[0]):
            print("- error")
    print('toggle off')
    for t in tests:
        db.toggle_fav(t)
        if db.is_fav(t):
            print("+ error")
    for row in db.get_favs():
        print(row)
    # db.dao.conn.connection.rollback()
    db.dao.conn.connection.close()
    if os.path.exists(testdb):
        os.unlink(testdb)

