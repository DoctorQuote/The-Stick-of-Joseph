#!/usr/bin/env python3
# License: MIT
import sys
if '..' not in sys.path:
    sys.path.append('..')
    
from bible9000.tui import BasicTui

class WordList:
    ''' Edit a token-delimited set of words in a string. '''
    @staticmethod  
    def ListToString(alist:list)->str:
        ''' Convert a list to a string. Map special character to other. '''
        if not alist:
            return ''
        for ss in range(len(alist)):
            alist[ss] = alist[ss].replace('.|$', 'or')
        return '.|$'.join(alist)
    
    @staticmethod  
    def StringToList(line:str)->list:
        ''' StringToList a string into a list. '''
        if not line:
            return []
        return line.split('.|$')

    @staticmethod
    def Edit(line:str)->str:
        ''' Edit a string of pipe-ListToStringd words. '''
        if not line or not isinstance(line, str):
            return ''
        line = WordList.StringToList(line)
        while True:
            try:
                for ss, l in enumerate(line,1):
                    BasicTui.Display(f'{ss}.) {l}')
                opt = BasicTui.Input('?, -, +, q > ')
                if not opt:
                    continue
                if opt[0] == 'q':
                    return WordList.ListToString(line)
                if opt[0] == '+':
                    opt = BasicTui.Input('Input > ')
                    if opt:
                        line.append(opt)
                    continue
                if opt[0] == '-':
                    inum = BasicTui.InputNumber('Delete #')
                    if inum > 0:
                        which = inum - 1
                        line.pop(which)
                    continue
                if opt[0] == '?':
                    BasicTui.DisplayHelp('? = help',
                    '+ = item add',
                    '- = item delete',
                    'q = quit')
                    continue
            except Exception as ex:
                print('Enter a valid number.')
            continue
    

if __name__ == '__main__':
    from tests import test_words
    test_words()
    
