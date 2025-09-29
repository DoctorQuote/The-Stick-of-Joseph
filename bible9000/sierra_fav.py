#!/usr/bin/env python3
# License: MIT
# Status: Testing Success.
import sqlite3
import sys
if '..' not in sys.path:
    sys.path.append('..')
from bible9000.tui import BasicTui
from bible9000.sierra_dao import SierraDAO

class FavDAO():
    ''' Manage the Fav Table '''
    def __init__(self, row=None):
        self.item = 0
        try:
            if row and isinstance(row, int):
                self.item = row
            elif row and isinstance(row, tuple):
                self.item = int(row[0])
            elif row and isinstance(row, str):
                self.item = int(row)
        except:
            pass


    def __repr__(self)->str:
        ''' Representational type includes an oid. '''
        v = vars(self)
        v['oid'] = 'FavDAOv1'
        return str(v)

    def merge(self, obj)->bool:
        ''' Restore a favorite selection. '''
        if obj and isinstance(obj, FavDAO):
            if not self.is_fav(obj.item):
                self.toggle_fav(obj.item)
            return True
        return False

    @staticmethod
    def Repr(rstr:dict):
        ''' Return an instance from a string or a dict
            tagged by repr(), else None. '''
        obj = rstr
        if isinstance(rstr, str):
            obj = eval(rstr)
        if isinstance(obj, dict):
            if 'oid' in obj and obj['oid'] == 'FavDAOv1':
                return FavDAO(tuple(obj.values()))
        return None
            
    @staticmethod
    def GetDAO(bSaints=False, database=None):
        ''' Connect to the database & return the DAO '''
        if not database:
            from bible9000.admin_ops import get_database
            database = get_database()
        result = FavDAO()
        result.dao = SierraDAO.GetDAO(bSaints, database)
        return result
    
    def toggle_fav(self, sierra)->bool:         
        if self.is_fav(sierra):
            cmd = f'DELETE From SqlFav WHERE item = {sierra};'
        else:
            cmd = f'INSERT INTO SqlFav (item) VALUES ({sierra});'
        self.dao.conn.execute(cmd)
        self.dao.conn.connection.commit()
        return True
    
    def is_fav(self, sierra)->bool:
        if isinstance(sierra, FavDAO):
            sierra = sierra.item
        cmd = f'SELECT * from SqlFav WHERE item = {sierra};'
        res = self.dao.conn.execute(cmd)
        if res and res.fetchone():
            return True
        return False
        
    def get_favs(self):
        ''' Get all favorites. '''
        cmd = f"SELECT * FROM SqlFav ORDER BY item;"
        try:
            res = self.dao.conn.execute(cmd)
            for a in res:
                yield FavDAO(a)
        except Exception as ex:
            BasicTui.DisplayError(ex)
        return None


if __name__ == '__main__':
    from tests import test_favs
    test_favs()

