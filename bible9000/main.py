#!/usr/bin/env python3
'''
File: main.py
Problem Domain: Console Application
Status: Production
Revision: 1.0.0

MISSION
=======
Create a simple way to read & collect your favorite passages
using every operating system where Python is available.

NEXUS
----- 
Installer: https://pypi.org/project/Bible9000/
Project:   https://github.com/DoctorQuote/The-Stick-of-Joseph
Website:   https://mightymaxims.com/
'''

b81 = True

import sys
sys.path.append('..')
from bible9000.sierra_dao  import SierraDAO
from bible9000.sierra_note import NoteDAO
from bible9000.sierra_fav  import FavDAO
from bible9000.tui import BasicTui

def dum():
    BasicTui.Display('(done)')


def do_func(prompt, options, level):
    '''Menued operations. '''
    choice = None
    while choice != options[-1][0]:
        BasicTui.Display(level * 15)
        for o in options:
            BasicTui.Display(o[0], o[1])
        choice = BasicTui.Input(prompt)
        if not choice:
            continue
        choice = choice[0].lower()
        BasicTui.Display(f">> {choice}")
        for o in options:
            if o[0] == choice:
                BasicTui.DisplayTitle(o[1])
                o[2]()


def do_search_books():
    ''' Search books & read from results. '''
    BasicTui.Display("Example: +word -word")
    BasicTui.Display("Enter q to quit")
    inc = ''; count = 0
    words = BasicTui.Input("+/-words: ")
    for word in words.strip().split(' '):
        if not word or word == 'q':
            return
        if inc:
            inc += ' AND '
        if word[0] == '-':
            inc += f'VERSE NOT LIKE "%{word[1:]}%"'
            count += 1
        if word[0] == '+':
            inc += f'VERSE LIKE "%{word[1:]}%"'
            count += 1
    if not count:
        return
    dao = SierraDAO.GetDAO(b81)
    BasicTui.Display(inc)
    sigma = 0
    for row in dao.search(inc):
        sigma += 1
        BasicTui.DisplayVerse(row)
    BasicTui.DisplayTitle(f"Found {sigma} Verses")


def do_list_books():
    ''' Displays the books. Saint = superset. Returns number
        of books displayed to permit selections of same.
    '''
    return BasicTui.DisplayBooks()


def do_random_reader()->int:
    ''' Start reading at a random location.
        Return the last Sierra number shown.
    '''
    dao = SierraDAO.GetDAO(b81)
    res = dao.conn.execute('SELECT COUNT(*) FROM SqlTblVerse;')
    vmax = res.fetchone()[0]+1
    import random    
    sierra = random.randrange(1,vmax)
    return browse_from(sierra)


def do_sierra_reader()->int:
    ''' Start reading at a Sierra location.
        Return the last Sierra number shown.
        Zero on error.
    '''
    books = []
    for row in SierraDAO.ListBooks(b81):
        books.append(row['book'].lower())
    last_book = do_list_books()
    option = BasicTui.Input('Book # > ')
    try:
        inum = int(option)
        if inum < 1 or inum > last_book:
            return
        ubook = books[inum-1]
        BasicTui.Display(f'Got {ubook}.')
        vrange = SierraDAO.GetBookRange(inum)
        option = BasicTui.Input(f'Enter a number between {vrange}, inclusive. > ')
        return browse_from(int(option))               
    except:
        return 0


def do_classic_reader():
    ''' Start browsing by classic chapter:verse. '''
    BasicTui.DisplayBooks()
    try:
        ibook = int(BasicTui.Input("Book #> "))
        ichapt = int(BasicTui.Input("Chapter #> "))
        iverse = int(BasicTui.Input("Verse #> "))
        dao = SierraDAO.GetDAO(b81)
        for res in dao.search(f'BookID = {ibook} AND BookChapterID = {ichapt} AND BookVerseID = {iverse}'):
            browse_from(dict(res)['sierra'])
    except Exception as ex:
        BasicTui.DisplayError(ex)


def edit_notes(sierra):
    sierra = int(sierra)
    notes = []
    dao = NoteDAO.GetDAO(b81)
    for ss, note in enumerate(dao.notes_for(sierra),1):
        line = f'{ss}.) {note[5]}'
        BasicTui.Display(line)
        notes.append(note)
    try:
        inum = int(BasicTui.Input("Number to edit > ")) - 1
        if inum >= len(notes):
            raise Exception()
        znote = BasicTui.Input('Notes: ').strip()
        if not znote:
            ok = BasicTui.Input('Delete Note (N/y) ?').strip()
            if ok and ok.lower()[0] == 'y':
                print('okeeeee')
                dao.delete_note(notes[inum][0])
                BasicTui.Display('Note deleted.')
                return
            else:
                raise Exception()
        dao.update_note(notes[inum][0], znote)
        BasicTui.Display('Note updated.')
    except:
        BasicTui.Display('done')


def make_note(sierra):
    sierra = int(sierra)
    BasicTui.Display("Use .edit. to fix notes")
    notes = BasicTui.Input('Notes: ').strip()
    if not notes:
        BasicTui.Display("No note.")
        return
    if notes == '.edit.':
        edit_notes(sierra)
        return
    dao = NoteDAO.GetDAO(b81)
    dao.insert_note(sierra, notes)
    BasicTui.Display(f"Note added for {sierra}.")
    
    
def browse_from(sierra)->int:
    ''' Start reading at a Sierra location.
        Return the last Sierra number shown.
        Zero on error.
    '''
    sierra = int(sierra)
    dao = SierraDAO.GetDAO(b81)
    res = dao.conn.execute('SELECT COUNT(*) FROM SqlTblVerse;')
    vmax = res.fetchone()[0]+1
    
    verse = dict(*dao.search_verse(sierra))
    option = ''
    while option != 'q':
        if not BasicTui.DisplayVerse(verse):
            return 0
        # do_func too much for a reader, methinks.
        option = BasicTui.Input('?, *, @, n, p, [q]uit > ').strip()
        if not option:
            option = 'n'
        try:
            o = option[0]
            if o == '?':
                BasicTui.DisplayTitle('HELP')
                BasicTui.Display('? = help')
                BasicTui.Display('* = toggle star')
                BasicTui.Display('@ = manage notes')
                BasicTui.Display('n = next page')
                BasicTui.Display('p = last page')
                BasicTui.Display('q = quit')
                continue
            if o == '*':
                BasicTui.DisplayTitle('STAR')
                fdao = FavDAO.GetDAO()
                fdao.toggle_fav(sierra)
                if fdao.is_fav(sierra):
                    BasicTui.Display(f'Starred {sierra}!')
                else:
                    BasicTui.Display(f'De-starred {sierra}.')
                continue
            if o == '@':
                BasicTui.DisplayTitle('NOTE')
                make_note(sierra)
                continue
            elif o == 'p':
                if sierra == 1:
                    BasicTui.Display('At the top.')
                    continue
                sierra -= 1
                verse = dict(*dao.search_verse(sierra))
            elif o == 'q':
                return sierra
            else: # default is 'n'
                if sierra == vmax:
                    BasicTui.Display('At the end.')
                    continue
                sierra += 1
                verse = dict(*dao.search_verse(sierra))
        except Exception as ex:
            BasicTui.DisplayError(ex)
            return sierra

def show_verse(sierra):
    dao = SierraDAO.GetDAO(b81)
    verse = dict(*dao.search_verse(sierra))
    BasicTui.DisplayVerse(verse)    

def do_search_stars():
    dao = FavDAO.GetDAO()
    count = 0
    for fav in dao.get_favs():
        count += 1
        show_verse(fav[0])
    BasicTui.DisplayTitle(f'There are {count} Stars.')
    
def do_search_notes():
    dao = NoteDAO.GetDAO()
    count = 0
    for fav in dao.get_notes():
        count += 1
        show_verse(fav[1])
    BasicTui.DisplayTitle(f'There are {count} Notes.')


def mainloop():
    ''' Core and functions. '''
    b81 = True
    options = [
        ("b", "List Books", do_list_books),
        ("v", "Sierra Reader", do_sierra_reader),
        ("c", "Classic Reader", do_classic_reader),
        ("r", "Random Reader", do_random_reader),
        ("s", "Search", do_search_books),
        ("@", "Notes", do_search_notes),
        ("*", "Stars", do_search_stars),
        ("q", "Quit", dum)
    ]
    BasicTui.SetTitle('The Stick of Joseph')
    do_func("Main Menu: ", options, ' ')
    BasicTui.Display(".")
    exit(0)
    

