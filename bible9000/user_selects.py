import sys
if '..' not in sys.path:
    sys.path.append('..')
    
from bible9000.sierra_dao  import SierraDAO
from bible9000.sierra_note import NoteDAO
from bible9000.sierra_fav  import FavDAO

class UserSelects:
    ''' Combine everything required for reporting. '''
    def __init__(self):
        sierra = 0
        is_fav = False
        text = ''
        chapter = 0
        verse = 0
        book = ''
        notes = ''
        subjects = ''

    def is_null(self):
        ''' See if instance is usable '''
        return bool(self.sierra) or bool(self.verse_row)

    @staticmethod
    def Lookup(sierra):
        ''' Realize the presently relevant user selection(s) '''
        result = UserSelects()
        result.sierra    = sierra
        verse_row = list(SierraDAO.GetDAO(True).search_verse(sierra))[0]
        result.text      = verse_row['text']
        result.chapter   = verse_row['chapter']
        result.verse     = verse_row['verse']
        result.book      = verse_row['book']
        note_row         = SierraDAO.GetNotes(sierra)
        result.notes     = note_row.Notes
        result.subjects  = note_row.Subject
        result.is_fav    = FavDAO.IsFav(sierra)
        return result

    @staticmethod
    def GetSelections()->list:
        ''' Get the sierra numbers of all user notes & favs '''
        dao = NoteDAO.GetDAO()
        count = 0
        verses = set()
        for fav in dao.get_all():
            verses.add(fav.vStart)
        dao = FavDAO.GetDAO()
        for fav in dao.get_favs():
            verses.add(fav.item)
        return sorted(verses)


    @staticmethod
    def Get():
        results = []
        for verse in UserSelects.GetSelections():
            results.append(UserSelects.Lookup(verse))
        return results


if __name__ == '__main__':
    print(UserSelects.GetSelections())
    for row in UserSelects.Get():
        print(vars(row))
        print('*'*10)


