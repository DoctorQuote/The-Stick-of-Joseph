from bible9000.pannel import Panel
lwrap = Panel()

class BasicTui:

    @staticmethod
    def SetTitle(title:str):
        line = ' '.join(('~'*3, title, '~'*3)).center(34)
        print('~'*len(line))
        print(line)
        print('~'*len(line))

    @staticmethod
    def IsAnsi():
        ''' Best-guess to see if we'lwrape ANSI colors. '''
        import sys
        if sys.platform == 'win32':
            return False
        return sys.stdout.isatty()
    
    @staticmethod
    def ClearScreen():
        ''' No ANSI codes assumed here, so we'll scroll. '''
        for _ in range(30):
            print()
    
    @staticmethod
    def Input(prompt:str)->str:
        ''' Great for testing. '''
        return input(prompt)

    @staticmethod
    def DisplayTitle(title:str, char='*'):
        ''' Common UI. '''
        print(lwrap.wrap(char * lwrap._wrap.width)[0])
        for zline in lwrap.wrap(title.strip()):
            print(zline)
        print(lwrap.wrap(char * lwrap._wrap.width)[0])
        
    @staticmethod
    def DisplayBooks(bSaints=True):
        ''' Displays the books. Saint = superset. Returns number
            of books displayed to permit selections of same.
        '''
        from bible9000.sierra_dao import SierraDAO
        for ss, book in enumerate(SierraDAO.ListBooks(bSaints),1):
            if(ss % 3) == 0:
                print(f"{ss:02}.) {book['book']:<18}")
            else:
                print(f"{ss:02}.) {book['book']:<18}", end = '')
        print()
        return ss
       
    @staticmethod
    def DisplayError(line:str)->bool:
        ''' Common display for all errors. '''
        return BasicTui.Display(str(line))
    
    @staticmethod
    def Display(*args)->bool:
        ''' Common display for all lines. '''
        if not args:
            return False
        line = ' '.join(args)
        for zline in lwrap.wrap(line.strip()):
            print(zline)
        return True

   
    @staticmethod
    def DisplayVerse(row:dict)->bool:
        ''' Common display for all verses. '''
        from bible9000.sierra_note import NoteDAO
        from bible9000.sierra_fav  import FavDAO
        if not row:
            print('[null]')
            return False
        line = row['text']
        
        print(lwrap.center(' {0} {1}:{2} '.format(
            row['book'],row['chapter'],row['verse']), '='))
        left = []
        for zline in lwrap.wrap(line.strip()):
            left.append(zline)
        right = []
        dao = FavDAO.GetDAO(True)
        if dao.is_fav(row['sierra']):
            right.append(*lwrap.wrap('* Starred *'))
        dao = NoteDAO.GetDAO(True)
        for note in dao.notes_for(row['sierra']):
            for zline in lwrap.wrap(note[5].strip()):
                right.append(zline)
        ll=len(left);lr = len(right)
        if not lr:
            for zline in left:
                print(zline)
        else:
            space = 0
            for ss in range(max(ll,lr)):
                if ss < ll:
                    space = len(left[ss])
                    print(left[ss], end='')
                elif ss >= ll:
                    print(' '* space, end='') 
                if ss < lr:
                    print(right[ss], end='')
                print()
                           
        print(lwrap.center(' Sierra Bible #{0} '.format(
            row['sierra']), '='))
        return True
   
    @staticmethod
    def DisplayVerseO(row:dict)->bool:
        ''' Common display for all verses. '''
        if not row:
            print('[null]')
            return False
        line = row['text']
        print(lwrap.center(' {0} {1}:{2} '.format(
            row['book'],row['chapter'],row['verse']), '='))
        for zline in lwrap.wrap(line.strip()):
            print(zline)
        print(lwrap.center(' Sierra Bible #{0} '.format(
            row['sierra']), '='))
        return True
