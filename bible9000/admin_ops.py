import os, os.path
import sys
if '..' not in sys.path:
    sys.path.append('..')
from bible9000.sierra_dao import SierraDAO
from bible9000.tui        import BasicTui

tables = {
    'SqlTblVerse':'CREATE TABLE IF NOT EXISTS SqlTblVerse (ID Integer PRIMARY KEY AUTOINCREMENT, BookID int, BookChapterID int, BookVerseID int, Verse String, VerseType int);',
    'SqlNotes'   :'CREATE TABLE IF NOT EXISTS SqlNotes (ID Integer PRIMARY KEY AUTOINCREMENT, vStart int, vEnd int, kwords String, Subject String, Notes String, NextId int);',
    'SqlBooks'   :'CREATE TABLE IF NOT EXISTS SqlBooks (ID Integer PRIMARY KEY AUTOINCREMENT, Book String, BookMeta String);',
    'SqlFav'     :'CREATE TABLE IF NOT EXISTS SqlFav   (item Integer);',
    }
    

def do_data_export():
    BasicTui.DisplayTitle("work in progress.")


def do_data_import():
    BasicTui.DisplayTitle("work in progress.")


def do_report_html():
    BasicTui.DisplayTitle("work in progress.")


def do_data_dump():
    BasicTui.DisplayTitle("work in progress.")


def do_data_restore():
    BasicTui.DisplayTitle("work in progress.")


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
    from sierra_note import NoteDAO
    from words import WordList
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
                

def do_admin_ops():
    from bible9000.main import do_func, dum
    ''' What users can do. '''
    options = [
        ("o", "Data Export (w.i.p)", do_data_export),
        ("i", "Data Import (w.i.p)", do_data_import),
        ("#", "HTML Report (w.i.p)", do_report_html),
        ("$", "Data Dump   (w.i.p)", do_data_dump),
        ("&", "Data Restore(w.i.p)", do_data_restore),
        ("q", "Quit", dum)
    ]
    do_func("Administration: ", options, '> Admin Menu')        


if __name__ == '__main__':
##    if consolidate_notes() == True:
##        print("Consolidated.")
    do_admin_ops()

