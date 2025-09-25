#!/usr/bin/env python3
import sys
sys.path.append('..')
from bible9000.tui import BasicTui

class WordList:
    ''' Edit a pipe-delimited set of words in a string. '''
    @staticmethod  
    def Encode(alist:list)->str:
        ''' Convert a list to a string. '''
        return '|'.join(alist)
    
    @staticmethod  
    def Decode(line:str)->list:
        ''' Decode a string into a list. '''
        return line.split('|')

    @staticmethod
    def Edit(line:str)->str:
        ''' Edit a string of pipe-Encoded words. '''
        if not line or not isinstance(line, str):
            return ''
        line = WordList.Decode(line)
        while True:
            try:
                for ss, l in enumerate(line,1):
                    BasicTui.Display(f'{ss}.) {l}')
                opt = BasicTui.Input('?, -, +, q > ').strip()
                if not opt:
                    continue
                if opt[0] == 'q':
                    return WordList.Encode(line)
                if opt[0] == '+':
                    opt = BasicTui.Input('Input > ').strip()
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
    lines = WordList.Edit(None)
    lines = WordList.Edit('')
    zin = 'able|"baker"|charley|delta|zulu'
    lines = WordList.Edit(zin)
    print(lines)
    
