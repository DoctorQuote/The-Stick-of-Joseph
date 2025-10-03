# License: MIT
# The Stick of Joseph
# This is the big, beautiful, free book.
# > Includes article tags!
# > Includes article links!

'''
<!-- HTML expression for copying an article link to clipboard -->
<input type="text" id="articleLink" value="https://example.com/your-article" readonly>
<button onclick="navigator.clipboard.writeText(document.getElementById('articleLink').value)">Copy Link</button>
'''
import os, os.path, sys
if '..' not in sys.path:
    sys.path.append('..')

import sqlite3
from bible9000.sierra_dao  import SierraDAO
from bible9000.sierra_note import NoteDAO
from bible9000.sierra_fav  import FavDAO


HEADER = """<html>
<head>
<meta charset="UTF-8">
<style>
body {
    font-size: 18px;
}
</style>
</head>
<body>
<h2>The Stick of Joseph</h2>
<b><i>Ancient Prophsey for Modern Times.</i></b>

<table width=450><tr><td>
<hr>
<center><i>1st Edition - 2025/10/01</i></center>
<hr>
<p>Welcome to the <b>"The Stick of Joseph"</b>!</p>

<h3>Introduction</h3>
<p>Look around the planet - no one can lay any creditable 
claim to fulfilling Ezekiel 37:16 - 18, John 10:16,
and 3 Nephi 15:21 ... let alone so much other
prophesy.</p>

<p>Indeed, reading the "Old Testament" won't many anyone a Jew, 
neither the "New Testament" anyone a Lutheran, nor "Another 
Testament" anyone Morman.</p>
<p>No: The <b>Stick of Joseph</b> was ever about 
one flock, one faith, and one founder ... <i>never one church.</i></p>
<p>Sharing sage advice then, as it remains now, is indeed caring.</p>

<h3>&#127760; The Sharing Globe</h3>
<p>
Clicking the &#127760; before any verse will 'be-browser
a link to every.</p>
<p>You might copy, paste, bookmark, email or
otherwise share that link with your family, friends &amp;
communities.
</p>
<p>
The link will work in every 'online' galaxy... and
'Devals notwithstanding.
<p>
<a href="https://MightyMaxims.com">Website</a><br>
<a href="https://ko-fi.com/doctorquote">Community</a><br>
<a href="https://github.com/DoctorQuote/The-Stick-of-Joseph">Project</a>
</p>
</td></tr></table>
"""

FIELDS = ['uid','vStart','vEnd','kWords',
    'Subject','Notes','NextId','Sierra','BookID',
    'ChaptID','VerseID','Verse']

# Define the paths for your input and output database files

def __write_html(output_html_file, line, books, fh):
    book = books[int(line['BookID'])-1]['book']
    classic_ref = f" {book} {line['ChaptID']}:{line['VerseID']}"          
    aref = f'<a href="{output_html_file}#\
{line["uid"]}">&#127760;</a>'           
    rec = f"<article id='{line['uid']}'>"
    rec += "<br><table width=450 border='1' cellpadding='10'>"
    rec += "<tr>"
    rec += "<td bgcolor='blue'>"
    rec += aref
    rec += f"&nbsp;<font color='yellow'>\
Verse #{line['Sierra']}.</font>\
<font color='gold'>{classic_ref}</font>\
<br><font color='white'>{line['Notes']}</font>"
    rec += "</td>"
    rec += "</tr>"
    rec += "<tr>"
    rec += "<td bgcolor='gray' height='55px'>"
    rec += "<font size='3' color='yellow'>"
    rec += line['Verse']
    rec += "</font>"
    rec += "</td>"
    rec += "</tr>"
    rec += "</table>"
    rec += "</article>"
    print(rec, file=fh)

def write_user_notes(output_html_file, quotes):
    books = list(SierraDAO.ListBooks(True))
    subjects = NoteDAO.GetSubjects()
    dreport = dict()
    for s in subjects:
        dreport[s] = list()
    dreport[None] = list()
    for quote in quotes:
        qdict = dict(zip(FIELDS, quote))
        if not subjects:
                # TODO: vNext ordering some day ...
                dreport[None].append(qdict)
                continue
        for subject in subjects:
            if subject in qdict['Subject']:
                # TODO: vNext ordering some day ...
                dreport[subject].append(qdict)               
    with open(output_html_file, 'w', encoding="utf8") as fh:
        print(HEADER, file=fh)
        if dreport:
            for zkey in dreport:
                if not zkey:
                    print('<br><center><hr>General Subjects<br></center>',file=fh)
                else:
                    print(f'<br><center><hr>{zkey}<br></center>',file=fh)
                for tup in dreport[zkey]:
                    __write_html(output_html_file, tup, books, fh)
        else:
            for tup in quotes:
                __write_html(output_html_file, tup, books, fh)
        print("<br><br><hr><hr><br></body>", file=fh)
        print("</html>", file=fh)


def export_notes_to_html(output_html_file = 'MyNotes.html'):
    ''' Generate lessons based upon SqlNotes. '''
    try:
        # Connect to the source database
        dao = SierraDAO.GetDAO()
        # Fix 'Could not decode to UTF-8 column' errors:
        # source_conn.text_factory = lambda b: b.decode('latin-1')

        # Select data from the 'authors' table
        authors_data = list(dao.conn.execute(
            "SELECT * from SqlNotes JOIN \
SqlTblVerse as e WHERE vStart == e.ID \
ORDER BY e.ID;"))
        write_user_notes(output_html_file, authors_data)
        rfile = os.path.sep.join((
            os.getcwd(),
            output_html_file))
        print(f"HTML File '{rfile}' created.")

    except sqlite3.Error as e:
        print(f"An SQLite error occurred: {e}")

    finally:
        # Close both database connections
        if 'source_conn' in locals() and source_conn:
            source_conn.close()
        if 'target_conn' in locals() and target_conn:
            target_conn.close()


if __name__ == '__main__':
    export_notes_to_html()
