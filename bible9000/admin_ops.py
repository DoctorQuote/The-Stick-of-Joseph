# License: MIT
import os, os.path, sys, time
if '..' not in sys.path:
    sys.path.append('..')
from bible9000.sierra_dao import SierraDAO
from bible9000.tui        import BasicTui

from bible9000.sierra_note import NoteDAO
from bible9000.sierra_fav  import FavDAO

tables = {
    'SqlTblVerse':'CREATE TABLE IF NOT EXISTS SqlTblVerse (ID Integer PRIMARY KEY AUTOINCREMENT, BookID int, BookChapterID int, BookVerseID int, Verse String, VerseType int);',
    'SqlNotes'   :'CREATE TABLE IF NOT EXISTS SqlNotes (ID Integer PRIMARY KEY AUTOINCREMENT, vStart int, vEnd int, kwords String, Subject String, Notes String, NextId int);',
    'SqlBooks'   :'CREATE TABLE IF NOT EXISTS SqlBooks (ID Integer PRIMARY KEY AUTOINCREMENT, Book String, BookMeta String);',
    'SqlFav'     :'CREATE TABLE IF NOT EXISTS SqlFav   (item Integer);',
    }


def do_export_user_data(prefix=None)->str:
    ''' Export user's NOTES and FAV's. File name returned. '''
    fname = time.strftime("%Y%m%d-%H%M%S") + '.sbbk'
    if prefix:
        fname = prefix + fname
    count = 0
    with open(fname, 'w') as fh:
        dao = NoteDAO.GetDAO(True)
        for row in dao.get_all():
            print(repr(row), file=fh)
            count += 1
        dao = FavDAO.GetDAO(True)
        for row in dao.get_favs():
            print(repr(row), file=fh)
            count += 1
    BasicTui.Display(f"Exported {count} items into {fname}.")
    return fname


def do_import_user_data()->bool:
    ''' Import user's NOTES and FAV's '''
    files = []
    for filename in os.listdir('.'):
        if filename.endswith(".sbbk"):
            files.append(filename)
    for ss, file in enumerate(files,1):
        print(f'{ss}.) {file}')
    inum = BasicTui.InputNumber('Restore #> ') -1
    if inum < 0 or inum >= len(files):
        return False
    with open(files[inum]) as fh:
        ndao = NoteDAO.GetDAO(True)
        fdao = FavDAO.GetDAO(True)
        for ss, _str in enumerate(fh, 1):
            obj = NoteDAO.Repr(_str)
            if obj:
                if not ndao.merge(obj):
                    BasicTui.DisplayError(f'Unable to import #{ss}')
                continue # restore
            obj = FavDAO.Repr(_str)
            if obj:
                if not fdao.merge(obj):
                    BasicTui.DisplayError(f'Unable to import #{ss}')
                continue # restore
            BasicTui.DisplayError(f'Unable to restore #{ss}')            
    BasicTui.DisplayTitle(f"Restored {ss} items.")


def do_rename_user_export()->bool:
    ''' Rename an exported user's NOTES and FAV's archive '''
    files = []
    for filename in os.listdir('.'):
        if filename.endswith(".sbbk"):
            files.append(filename)
    for ss, file in enumerate(files,1):
        print(f'{ss}.) {file}')
    inum = BasicTui.InputNumber('Rename #> ') -1
    if inum < 0 or inum >= len(files):
        return False
    zname = BasicTui.Input('New file name > ')
    if not zname:
        BasicTui.Display('Not renamed.')
        return False
    if not zname.endswith('.sbbk'):
        zname += '.sbbk'
    if os.path.exists(zname):
        BasicTui.Display(f'Whoops - {zname} already exists.')
        op = BasicTui.Input(f'Ok to overwrite {zname}? y/N > ')
        if not op or op[0] != 'y':
            BasicTui.Display(f'File {zname} not changed.')
            return False
    os.rename(files[inum], zname)
    BasicTui.DisplayTitle(f"Exported {zname} - share and enjoy.")
    return True


def get_database():
    ''' Get the installed database location. '''
    pdir = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(pdir, 'biblia.sqlt3')


def destory_notes_and_fav():
    ''' Handy when cleaning-up after r&d (etc.) '''
    dao = SierraDAO.GetDAO()
    for key in 'SqlNotes', 'SqlFav':
        dao.conn.execute(f'DROP TABLE IF EXISTS {key};')
        dao.conn.execute(tables[key])
    dao.conn.connection.commit()


def cleanup():
    ''' Tightent-up / vacuum the database. '''
    dao = SierraDAO.GetDAO()
    dao.conn.execute('vacuum')
    dao.conn.connection.commit()


def create_tables():
    ''' Create requesite tables iff they do not already exist. '''
    dao = SierraDAO.GetDAO()
    for key in tables:
        dao.conn.execute(tables[key])
    dao.conn.connection.commit()

    
def destroy_notes():
    ''' Re-create the SqlNotes Table from scratch. Will destroy SqlNotes!'''
    key = 'SqlNotes'
    dao = SierraDAO.GetDAO()
    dao.conn.execute(f'DROP TABLE IF EXISTS {key};')
    dao.conn.execute(tables[key])
    dao.conn.connection.commit()
    

def destroy_everything():
    ''' Re-create the database from scratch. Will destroy SqlNotes!'''
    import os.path
    ''' My thing - not to worry. '''
    zfile = r'C:\d_drive\USR\code\TheBibleProjects\TheBibleProjects-main\SierraBible\biblia\b1.tab'
    if not os.path.exists(zfile):
        return

    dao = SierraDAO.GetDAO()
    for key in tables:
        dao.conn.execute(f'DROP TABLE IF EXISTS {key};')
        dao.conn.execute(tables[key])

    vtags = ['zb','book','verse','text']
    books = dict()
    lines = []
    with open(zfile) as fh:
        for line in fh:
            row = line.split('\t')
            zd = dict(zip(vtags,row))
            if len(books) < 40:
                my_book = 'kjv.ot.'+ zd['book']
                books[zd['book']] = my_book
            elif len(books) < 67:
                my_book = 'kjv.nt.'+ zd['book']
                books[zd['book']] = my_book
            else:
                my_book = 'lds.bom.'+ zd['book']
                books[zd['book']] = my_book
            zd['book'] = [my_book, len(books)]
            zd['verse'] = zd['verse'][2:].split(':')
            lines.append(zd)
                
    print(len(lines), len(books))
    for ss, b in enumerate(books, 1):
        print(ss,books[b])
        cmd = f'insert into SqlBooks (Book) VALUES ("{books[b]}");'
        dao.conn.execute(cmd)
    for line in lines:
        cmd = f'''insert into SqlTblVerse
    (BookID, BookChapterID, BookVerseID, Verse) VALUES
    ({line['book'][1]}, {line['verse'][0]}, {line['verse'][1]}, "{line['text'].strip()}")
    ;'''
        dao.conn.execute(cmd)
    dao.conn.connection.commit()


def consolidate_notes():
    from bible9000.sierra_note import NoteDAO
    from bible9000.words import WordList
    dao = NoteDAO.GetDAO(True)
    notes = dict()
    for note in dao.get_notes():
        if not note.Sierra in notes:
            notes[note.Sierra] = []
        notes[note.Sierra].append(note)
    for sierra in notes:
        mega = NoteDAO()
        if len(notes[sierra]) > 1:
            # Got dups.
            sigma = set(); maga = set()
            for note in notes[sierra]:
                sigma.union(WordList.Decode(note.Subjects))
                maga.union(WordList.Decode(note.Notes))
            mega.Sierra = sierra
            if sigma:
                mega.Subject = WordList.Encode(list(sigma))
            if maga:
                mega.Notes = WordList.Encode(list(maga))
            # UNLOVED - Every other value is presently unused.
            if dao.insert_note(mega):
                # Junk the dups
                for old in notes[sierra]:
                    if not dao.delete(old):
                        dao.rollback()
                        return False
    return True


def do_user_db_reset()->bool:
    ''' Remove all user data. a bakcup_* file is created.'''
    opt = BasicTui.Input('Remove custom content? y/N > ')
    if not opt or opt[0] != 'y':
        BasicTui.Display('Nothing removed.')
        return False
    fname = do_export_user_data('backup_')
    if not fname:
        BasicTui.DisplayError(f'Unable to backup to {fname}.')
        BasicTui.Display('Nothing removed.')
        return False
    dao = SierraDAO.GetDAO()
    for key in ('SqlNotes', 'SqlFav'):
        dao.conn.execute(f'DROP TABLE IF EXISTS {key};')
        dao.conn.execute(tables[key])
    create_tables()
    BasicTui.Display(f'Database has been reset. Backup archive saved as {fname}.')
    return True


def do_user_admin_help():
    BasicTui.Display('o: [Data Export]')
    BasicTui.Display('Export your notes, stars, and subjects to \
an dated archive file name. A great way to save our work for later \
restorations. ')
    BasicTui.Display('~~~~~')
    BasicTui.Display('i: [Data Import]')
    BasicTui.Display('Merge any previously archived file into our work. \
PLEASE NOTE THAT notes on any EXISTING verse will be replaced!')
    BasicTui.Display('~~~~~')
    BasicTui.Display('r: [Rename Data Export]')
    BasicTui.Display('The best way to select, rename, and better \
share any previously exported data archive with others.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('!: [Database Reset]')
    BasicTui.Display('Resets the database. Removes all notes, \
favorites, and subjects.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('?: [Help]')
    BasicTui.Display('Show this menu :-)')
    BasicTui.Display('~~~~~')
    BasicTui.Display('q: [Return] to previous session.')
    BasicTui.Display('~~~~~')


def do_admin_ops():
    from bible9000.main import do_func, dum
    ''' What users can do. '''
    options = [
        ("o", "Data Export", do_export_user_data),
        ("i", "Data Import", do_import_user_data),
        ("r", "Rename Data Export", do_rename_user_export),
        ("!", "Reset Database", do_user_db_reset),
        ("?", "Help", do_user_admin_help),
        ("q", "Quit", dum)
    ]
    do_func("Administration: ", options, '>> Admin Menu')        


if __name__ == '__main__':
##    if consolidate_notes() == True:
##        print("Consolidated.")
    do_admin_ops()

