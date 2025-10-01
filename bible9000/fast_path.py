#!/usr/bin/env python3
'''
License: MIT
File: fast_path.py
Problem Domain: Console Application
'''

class FastPath:
    ''' Create a command stack. '''
    _present = None

    def __init__(self, path:str):
        ''' Save the stack. '''
        self.cmd = path.split('.')
        self.cmd.reverse()

    def pop(self):
        ''' Extract the next command from the stack. '''
        if len(FastPath._present.cmd):
            return FastPath._present.cmd.pop()
        return None

    @staticmethod
    def Setup(path:str=''):
        ''' Set-up the command stack. '''
        FastPath._present = FastPath(path)

    @staticmethod
    def Len():
        ''' Get the length of the command stack. '''
        if FastPath._present:
            return len(FastPath._present.cmd)
        return 0

    @staticmethod
    def Pop():
        ''' Remove the next command from the list. '''
        if FastPath._present:
            if len(FastPath._present.cmd):
                return FastPath._present.cmd.pop()


if __name__ == '__main__':
    FastPath.Setup('a.b.c.d')
    while FastPath.Len():
        print(FastPath.Pop())
