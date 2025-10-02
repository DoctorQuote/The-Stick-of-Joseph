#!/usr/bin/env python3
'''
License: MIT
File: main.py
Problem Domain: Console Application
'''

STATUS   = "Production"
VERSION  = "2.0.1"
MAX_FIND = 40 # When to enter 'tally only' mode

'''
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
if '..' not in sys.path:
    sys.path.append('..')

from bible9000.sierra_dao  import SierraDAO
from bible9000.sierra_note import NoteDAO
from bible9000.sierra_fav  import FavDAO
from bible9000.tui import BasicTui
from bible9000.words import WordList
from bible9000.fast_path import FastPath
from bible9000.report_html import export_notes_to_html
from bible9000.admin_ops import *

BOOKS    = SierraDAO.GetTestaments()

def dum():
    BasicTui.Display('(done)')


def do_func(prompt, options, level=None):
    '''Menued operations. '''
    choice = None
    while choice != options[-1][0]:
        if level:
            BasicTui.DisplayTitle(level)
        for o in options:
            BasicTui.Display(o[0], o[1])
        choice = BasicTui.Input(prompt)
        if not choice:
            continue
        if choice.find('.') != -1:
            FastPath.Setup(choice)
            choice = FastPath.Pop()
            if not choice:
                continue
        choice = choice[0].lower()
        BasicTui.Display(f">> {choice}")
        for o in options:
            if o[0] == choice:
                BasicTui.DisplayTitle(o[1])
                o[2]()


def do_search_subjects():
    ''' Mass-manage all subjects. '''
    dao = NoteDAO.GetDAO(b81)
    while True:
        subjects = dao.get_subjects_list()
        ss = 0; subject = None
        for ss, subject in enumerate(subjects,1):
            BasicTui.Display(f'{ss}.) {subject}')
        BasicTui.Display(f"Found {ss} Subjects.")
        if not ss:
            return
        which = BasicTui.InputNumber("Number: ")
        if which < 1 or which > len(subjects):
            BasicTui.DisplayError('Selection out of range.')
            return
        subject = subjects[which - 1]
        option = BasicTui.Input('?, r, d, or q > ')
        if not option: return
        if option[0] == '?':
            BasicTui.Display('? = Help (show this :-)')
            BasicTui.Display('r = Rename Subject')       
            BasicTui.Display('d = Delete Subject')
            BasicTui.Display('q = Return to previous')
            continue
        if option[0] == 'd':
            b = dao.subject_delete(subject)
            if b:
                print(f"Removed Subject '{subject}'")
            else:
                BasicTui.DisplayError(f"Error: Subject '{subject}' not deleted.")
            continue
        if option[0] == 'r':
            nname = BasicTui.Input(f'Rename "{subject}" to: ')
            b = dao.subject_rename(subject, nname)
            if b:
                print(f"Renamed Subject '{subject}'")
            else:
                BasicTui.DisplayError(f"Error: Subject '{subject}' not deleted.")
            continue
        return None


def do_search_books():
    ''' Search books & read from results. '''
    while True:
        yikes = False # search overflow
        BasicTui.Display("Example: +word -word, -a")
        BasicTui.Display("Enter q to quit")
        inc = ''; count = 0; exbook = {}
        words = BasicTui.Input("?, +w, -w, q: ")
        cols = words.strip().split(' ')
        for word in cols:
            if not word or word == 'q':
                return
            if word == '?':
                BasicTui.DisplayHelp("?  = help",
                    "+w = include word",
                    "-w = exclude word",
                    "-o = exclude testament, old",
                    "-n = exclude testament, new",
                    "-a = exclude testament, another")
                break
            if word in ['-o', '-n', '-a']:
                exbook[word[1]] = 0
                continue
            if inc:
                inc += ' AND '
            if word[0] == '-':
                inc += f'VERSE NOT LIKE "%{word[1:]}%"'
                count += 1
            if word[0] == '+':
                inc += f'VERSE LIKE "%{word[1:]}%"'
                count += 1
        if not count:
            continue
        dao = SierraDAO.GetDAO(b81)
        sigma = 0
        for row in dao.search(inc):
            if exbook:
                _id = row['book']
                if 'o' in exbook:
                    if BOOKS['ot'].count(_id): 
                        exbook['o'] += 1
                        continue
                if 'n' in exbook:
                    if BOOKS['nt'].count(_id): 
                        exbook['n'] += 1
                        continue
                if 'a' in exbook:
                    if BOOKS['bom'].count(_id): 
                        exbook['a'] += 1
                        continue
            sigma += 1
            if sigma == MAX_FIND:
                yikes = True
                BasicTui.DisplayError(f"Results >= {MAX_FIND} ...")
            if not yikes:
                BasicTui.DisplayVerse(row)
        if exbook:
            BasicTui.DisplayTitle("Omissions")
            for key in exbook:
                BasicTui.Display(f'{key} has {exbook[key]} matches.')
        BasicTui.DisplayTitle(f"Detected {sigma} Verses")


def do_list_books():
    ''' Displays the books. Returns number of books displayed
        to permit selections of same.
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
    inum = BasicTui.InputNumber('Book # > ')
    if inum > 0 and inum <= last_book:
        ubook = books[inum-1]
        BasicTui.Display(f'Got {ubook}.')
        vrange = SierraDAO.GetBookRange(inum)
        vnum = BasicTui.InputNumber(f'Book numbers {vrange} > ')
        return browse_from(vnum)               
    else:
        return 0


def do_classic_reader():
    ''' Start browsing by classic chapter:verse. '''
    BasicTui.DisplayBooks()
    ibook = BasicTui.InputNumber("Book #> ")
    if ibook == -1:
        BasicTui.DisplayError("Bad book number.")
        return
    ichapt = BasicTui.InputNumber("Chapter #> ")
    if ichapt == -1:
        BasicTui.DisplayError("Bad chapter number.")
        return
    iverse = BasicTui.InputNumber("Verse #> ")
    if iverse == -1:
        BasicTui.DisplayError("Bad verse number.")
        return
    dao = SierraDAO.GetDAO(b81)
    for res in dao.search(f'BookID = {ibook} AND BookChapterID = {ichapt} AND BookVerseID = {iverse}'):
        browse_from(dict(res)['sierra'])



def edit_notes(sierra, is_subject=False)->bool:
    ''' Manage the '.edit.' mode for any Sierra verse #. '''
    noun = 'Note'
    if is_subject:
        noun = 'Subject'
    sierra = int(sierra)
    dao = NoteDAO.GetDAO(b81)
    row = dao.note_for(sierra)
    if not row: return False
    notes = []
    data = row.Notes
    if is_subject:
        data = row.Subject
    for ss, n in enumerate(data,1):
        line = f'{ss}.) {n}'
        BasicTui.Display(line)
        notes.append(n)

    inum = BasicTui.InputNumber("Number to edit > ") - 1
    if inum < 0:
        return False
    znote = BasicTui.Input(f'{noun}: ')
    if not znote:
        ok = BasicTui.Input(f'Delete {noun} (N/y) ?')
        if ok and ok.lower()[0] == 'y':
            notes.pop(inum)
        else:
            return False
    else:
        notes[inum] = znote # edited
    if is_subject:
        row.Subject = notes
    else:
        row.Notes = notes
    if row.is_null():
        dao.delete_note(row)
    else:
        dao.update_note(row)
        
    BasicTui.Display(f'{noun} updated.')
    BasicTui.Display('done')
    return True


def manage_notes(sierra, is_subject=False):
    ''' Create, edit, and delete notes for any Sierra verse #. '''
    noun = 'Note'
    if is_subject:
        noun = 'Subject'
    sierra = int(sierra)
    BasicTui.Display(f"Use .edit. to fix {noun}s")
    notes = BasicTui.Input(f'{noun}s: ')
    if not notes:
        BasicTui.Display(f"No {noun}.")
        return
    if notes == '.edit.':
        return edit_notes(sierra, is_subject)
    dao = NoteDAO.GetDAO(b81)
    row = dao.note_for(sierra)
    if not row:
        row = NoteDAO()
    row.vStart = sierra
    if is_subject:
        row.add_subject(notes)
    else:
        row.add_note(notes)
    dao.insert_or_update_note(row)
    BasicTui.Display(f"{noun} added for {sierra}.")
    return True


def edit_subjects(sierra):
    ''' Associate 'subjects' with a Sierra verse.
        Subjects permit common 'topic threads' across
        a series of notes / stars. '''
    return edit_subjects(sierra, True)


def manage_subjects(sierra):
    ''' Create, edit, and delete subjects for any Sierra verse #. '''
    return manage_notes(sierra, True)

    
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
        option = BasicTui.Input('?, *, @, =, n, p, [q]uit > ')
        if not option:
            option = 'n'
        try:
            o = option[0]
            if o == '?':
                BasicTui.DisplayHelp('? = help',
                '* = toggle star',
                '@ = manage notes',
                '= = manage subjects',
                'n = next page',
                'p = last page',
                'q = quit')
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
                BasicTui.DisplayTitle('NOTES')
                manage_notes(sierra)
                continue
            if o == '=':
                BasicTui.DisplayTitle('SUBJECTS')
                manage_subjects(sierra)
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

 
def do_user_report():
    dao = NoteDAO.GetDAO()
    count = 0
    for fav in dao.get_all():
        count += 1
        show_verse(fav.vStart)
    BasicTui.DisplayTitle(f'There are {count} Notes.')


def do_report_html():
    export_notes_to_html()


def do_help_notes_main():
    BasicTui.Display('s: [Search]')
    BasicTui.Display('Search all, either, or none to simply count words.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('=: [Subjects]')
    BasicTui.Display('Display all subjects (pages) created so far.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('#: [Report]')
    BasicTui.Display('Display the Note report.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('$: [HTML Report]')
    BasicTui.Display('The HTML Report is a great way to share your \
notes, stars, and subjects with the rest of your world. Once exported \
your subject groups will combine the topics you feel can be \
presented together.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('?: [Help]')
    BasicTui.Display('Show this list.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('q: [Quit]')
    BasicTui.Display('Exit Notes.')
    BasicTui.Display('~~~~~')    


def notes_main():
    ''' Seaching & working with our notes. '''
    b81 = True
    options = [
        ("s", "Search", do_search_books),
        ("=", "Subjects", do_search_subjects),
        ("@", "Display Notes", do_user_report),
        ("#", "Export HTML", do_report_html),
        ("?", "Help", do_help_notes_main),
        ("q", "Quit", dum)
        ]
    do_func("Option: ", options, '> Notes Menu')
    BasicTui.Display(".")
    

def do_help_main():
    ''' Explain the main options. '''
    BasicTui.Display('b: [List Books]')
    BasicTui.Display('List the names of all books in the "Stick of Joseph".')
    BasicTui.Display('~~~~~')
    BasicTui.Display('v: [ Sierra Reader]')
    BasicTui.Display('Select a book to start reading by verse number.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('c: [Classic Reader]')
    BasicTui.Display("Select a book's number, chapter, and verse to start reading.")
    BasicTui.Display('~~~~~')
    BasicTui.Display('r: [Random Reader]')
    BasicTui.Display('See what fate might have you read today?')
    BasicTui.Display('~~~~~'),
    BasicTui.Display('n: [Report]'),
    BasicTui.Display('Notes & Searching.'),
    BasicTui.Display('~~~~~')
    BasicTui.Display('a: [Admin]')
    BasicTui.Display('Data import, export, and backup.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('?: [Help]')
    BasicTui.Display('Show this list.')
    BasicTui.Display('~~~~~')
    BasicTui.Display('q: [Quit]')
    BasicTui.Display('Program exit.')
    BasicTui.Display('~~~~~')


def mainloop():
    ''' TUI features and functions. '''   
    b81 = True
    options = [
        ("b", "List Books", do_list_books),
        ("v", "Sierra Reader", do_sierra_reader),
        ("c", "Classic Reader", do_classic_reader),
        ("r", "Random Reader", do_random_reader),
        ("n", "Notes", notes_main),
        ("a", "Admin", do_admin_ops),
        ("?", "Help", do_help_main),
        ("q", "Quit", dum)
    ]
    BasicTui.SetTitle('The Stick of Joseph')
    BasicTui.Display(STATUS, 'Version', VERSION)
    do_func("Main Menu: ", options, '# Main Menu')
    BasicTui.Display(".")
    
if __name__ == '__main__':
    mainloop()
