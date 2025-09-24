#!/usr/bin/env python3
'''
Installer: https://pypi.org/project/Bible9000/
Project:   https://github.com/DoctorQuote/The-Stick-of-Joseph
Website:   https://mightymaxims.com/
'''
try:
    import sys
    sys.path.append('..')
    from bible9000.main import mainloop
    mainloop()
except Exception as ex:
    print(ex)
    print('Beware the wumpus.')
#exit(0)
