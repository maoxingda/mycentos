#! /bin/python3
import os
import re
import sys
import readline
from abc import abstractmethod
from subprocess import call, check_output


# print(os.getcwd())
# os.chdir(os.path.expandvars('${HOME}'))
# print(os.getcwd())
# os.chdir(os.pardir)
# print(os.getcwd())
# sys.exit()


# class Shape:
#     def m1(self):
#         print('Shape')
#
#
# class Circle(Shape):
#
#     def m1(self):
#         print('Circle')
#
#
# shape = Shape()
# shape.m1()
# shape = Circle()
# shape.m1()
# sys.exit()

# class Person:
#
#     def __init__(self, aid):
#         self._id = aid
#
#
# class Student(Person):
#
#     def __init__(self, aid, name, age):
#         super().__init__(aid)
#         self.__name = name
#         self.age = age
#
#     def __str__(self) -> str:
#         return f'Student({self._id}, {self.__name}, {self.age})'
#
#
# s1 = Student(1, 'maoxd', 30)
# print(s1)
# # print(s1.__name)
# print(s1._id)
# print(s1.age)
# sys.exit()


class XPath(object):
    curdir = '/'

    @staticmethod
    def getcwd():
        return XPath.curdir

    @staticmethod
    def join(path):
        return os.path.join(XPath.curdir, path)

    @staticmethod
    def chdir(path):
        if path == '.':
            XPath.curdir = os.path.dirname(XPath.curdir)
        elif path == '..':
            XPath.curdir = os.path.dirname(XPath.curdir)
            XPath.curdir = os.path.dirname(XPath.curdir)
        elif path == 'cd':
            XPath.curdir = '/'
        elif path.startswith('cd '):
            XPath.curdir = os.path.join(XPath.curdir, path[3:])


def dir_layout():
    local_bname = os.path.basename(os.getcwd())
    if local_bname == '':
        local_bname = '/'

    remote_bname = os.path.basename(XPath.getcwd())
    if remote_bname == '':
        remote_bname = '/'

    prompt = '>'
    if mode == 2:
        prompt += '>>'

    return f'\n[{local_bname}:{remote_bname}]{prompt} '


def mk_remote_choices():
    rchs = check_output(['hdfs', 'dfs', '-ls', '-C', XPath.getcwd()]).decode('utf-8').split('\n')[:-1]
    return [rch.split('/')[-1] for rch in rchs]


def catcmd(acmd, *args):
    return acmd + ' ' + ' '.join(args)


def logcall(acmd):
    if mode == 2:
        if enable_log:
            print(acmd)
        call(acmd, shell=True)
    else:
        if enable_log:
            print(f'>>> hdfs dfs -{acmd}')
        call(f'hdfs dfs -{acmd}', shell=True)


def completer(text, state):
    # options = [x for x in ['exit'] + local_choices + remote_choices if x.find(text) != -1]
    options = [x for x in ['exit'] + local_choices + remote_choices if x.startswith(text)]
    try:
        return options[state]
    except IndexError:
        return None


def unkown_cmd(acmd):
    print(f'unkown command ---> {acmd}')


def choice_layout():
    if enable_log:
        if len(local_choices) > 0:
            print('\n=============local choices=============')
        for choice in local_choices:
            print(choice, end=' ')

        if len(remote_choices) > 0:
            print('\n\n=============remote choices============')
        for choice in remote_choices:
            print(choice, end=' ')

        if len(local_choices) > 0 or len(remote_choices) > 0:
            print()
    # print(readline.get_completer_delims())


def join(path):
    return os.path.join(os.getcwd(), path)


def mkdir():
    global remote_choices
    what = re.search(r'^\s*md\s+'
                     r'(?P<option>(-p\s+)?)'
                     r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', cmd)
    if what:
        option = what.group('option')
        root = what.group('root')
        arg = what.group('prefix') + what.group('suffix')
        if root:
            if option:
                logcall(f'mkdir -p {arg}')
            else:
                logcall(f'mkdir {arg}')
        else:
            if option:
                logcall(f'mkdir -p {XPath.join(arg)}')
            else:
                logcall(f'mkdir {XPath.join(arg)}')
    remote_choices = mk_remote_choices()


def init():
    global local_choices, remote_choices

    remote_choices += mk_remote_choices()
    local_choices += check_output(['ls', os.getcwd()]).decode('utf-8').split('\n')[:-1]

    readline.set_completer(completer)
    readline.parse_and_bind("tab: complete")


def rmdir():
    global update, remote_choices

    what = re.search(r'^\s*rm\s+'
                     r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', cmd)

    update = True

    if what:
        root = what.group('root')
        arg = what.group('prefix') + what.group('suffix')

        if root:
            logcall(f'rm -r {arg}')
        else:
            logcall(f'rm -r {XPath.join(arg)}')

    # wildcard
    elif re.search(r'^\s*rm\s+\*\.\w+', cmd):
        logcall(cmd)
    else:
        update = False

    if update:
        remote_choices = mk_remote_choices()


def put():
    global cmd, remote_choices

    cmd = cmd[4:]

    if cmd[0] == '/':
        idx = cmd.find(' ')
        if idx != -1:
            if cmd[idx + 1] == '/':
                logcall(f'put {cmd}')
            else:
                logcall(f'put {cmd[:idx]} {XPath.join(cmd[idx + 1:])}')
        else:
            logcall(f'put {cmd} {XPath.getcwd()}')
    else:
        idx = cmd.find(' ')
        if idx != -1:
            if cmd[idx + 1] == '/':
                logcall(f'put {join(cmd[:idx])} {cmd[idx + 1:]}')
            else:
                logcall(f'put {join(cmd[:idx])} {XPath.join(cmd[idx + 1:])}')
        else:
            logcall(f'put {join(cmd)} {XPath.getcwd()}')

    remote_choices = mk_remote_choices()


class XXPath:

    def __init__(self, cwd, oldpwd):
        self.cwd = cwd
        self.oldpwd = oldpwd

    @property
    def cwd(self):
        return self.__cwd

    @cwd.setter
    def cwd(self, val):
        self.__cwd = val

    @property
    def oldpwd(self):
        return self.__oldpwd

    @oldpwd.setter
    def oldpwd(self, val):
        self.__oldpwd = val

    def cd(self, path):
        pass

    def cdpardir(self):
        pass

    def cdhome(self):
        pass

    def cdoldpwd(self):
        pass

    def chdir(self, acmd):
        # .
        if re.fullmatch(r'\s*\.\s*', acmd):
            self.cdpardir()
            self.oldpwd = self.cwd
            self.cwd = os.path.dirname(self.cwd)

        # ..
        elif re.fullmatch(r'\s*\.\.\s*', acmd):
            self.cdpardir()
            self.cdpardir()
            self.oldpwd = self.cwd
            self.cwd = os.path.dirname(self.cwd)
            self.cwd = os.path.dirname(self.cwd)

        # cd
        elif re.fullmatch(r'\s*cd\s*', acmd):
            self.oldpwd = self.cwd
            self.cdhome()

        # cd -
        elif re.fullmatch(r'\s*cd\s+-\s*', acmd):
            self.cdoldpwd()
            temp = self.oldpwd
            self.oldpwd = self.cwd
            self.cwd = temp
            pass

        # others
        else:
            what = re.fullmatch(r'\s*cd\s*'
                                r'(?P<root>/?)'
                                r'(?P<arg>\w+(?:/\w+)*)/?\s*', acmd)
            if what:
                self.oldpwd = self.cwd
                path = what.group('root') + what.group('arg')
                if not what.group('root'):
                    # convert to absolute path (e.g. /root or /root/bin)
                    path = os.path.join(self.cwd, path)
                self.cwd = path
                self.cd(path)
            else:
                unkown_cmd(acmd)


class LocalPath(XXPath):

    def __init__(self, cwd, oldpwd):
        super().__init__(cwd, oldpwd)

    def cd(self, path):
        os.chdir(path)

    def cdpardir(self):
        os.chdir(os.pardir)

    def cdhome(self):
        os.chdir(os.path.expandvars('${HOME}'))
        self.cwd = os.getcwd()

    def cdoldpwd(self):
        os.chdir(self.oldpwd)


class RemotePath(XXPath):

    def __init__(self, cwd, oldpwd):
        super().__init__(cwd, oldpwd)

    def cdhome(self):
        self.cwd = '/'


p = RemotePath('/', '/')
# p = LocalPath(os.getcwd(), os.getcwd())

while True:

    print(p.cwd)

    tcmd = input('>>> ')

    if tcmd == 'exit':
        sys.exit()

    p.chdir(tcmd)


if __name__ == '__main__':

    mode = 1
    update = True
    local_choices = []
    remote_choices = []
    unkown_cmd = 'unkown command'
    enable_log = len(sys.argv) > 1 and sys.argv[1] == '1'

    init()

    while True:

        choice_layout()
        cmd = input(dir_layout())

        try:
            if cmd == 'exit':
                if mode == 2:
                    mode -= 1
                elif mode == 1:
                    sys.exit()

            # system commands
            elif cmd == '!':
                mode = 2
            elif mode == 2:

                # change dir
                update = True
                if cmd == '.':
                    os.chdir(os.path.abspath(os.pardir))
                elif cmd == '..':
                    os.chdir(os.path.abspath(os.pardir))
                    os.chdir(os.path.abspath(os.pardir))
                elif cmd == 'cd':
                    os.chdir(os.environ['HOME'])
                elif cmd == 'cd -':
                    os.chdir(os.environ['OLDPWD'])
                elif cmd.startswith('cd '):
                    os.chdir(join(cmd[3:]))

                # others
                else:
                    logcall(cmd)
                    update = False

                if update is True:
                    local_choices = check_output(['ls', os.getcwd()]).decode('utf-8').split('\n')[:-1]

            # hdfs commands
            else:

                # change dir
                if cmd == 'pwd':
                    print(XPath.getcwd())
                elif re.search(r'^\s*(?:\.|\.\.|cd|cd\s+)', cmd):
                    XPath.chdir(cmd)
                    remote_choices = mk_remote_choices()

                # create dir
                elif re.search(r'^\s*md\s+', cmd):
                    mkdir()

                # touch file
                elif cmd.startswith('touch '):
                    pass

                # del dir or file
                elif re.search(r'^\s*rm\s+', cmd):
                    rmdir()

                # list dir content
                elif cmd == 'ls':
                    logcall(f'ls {XPath.getcwd()}')
                elif cmd == 'lsc':
                    logcall(f'ls -C {XPath.getcwd()}')
                elif cmd.startswith('ls '):
                    if cmd[3] == '/':
                        logcall(f'ls {cmd[3:]}')
                    else:
                        logcall(f'ls {XPath.join(cmd[3:])}')

                # cat file content
                elif cmd.startswith('cat '):
                    cmd = cmd[4:]
                    if cmd[0] == '/':
                        logcall(f'cat {cmd}')
                    else:
                        logcall(f'cat {XPath.join(cmd)}')

                # put or get file
                elif cmd.startswith('put '):
                    put()
                elif cmd.startswith('get '):
                    cmd = cmd[4:]
                    if cmd[0] == '/':
                        logcall(f'get {cmd}')
                    else:
                        logcall(f'get {XPath.join(cmd)}')

                # others
                else:
                    logcall(cmd)
        except Exception:
            pass

# Usage: hadoop fs [generic options]
# 	[-appendToFile <localsrc> ... <dst>]
# 	[-cat [-ignoreCrc] <src> ...]
# 	[-checksum <src> ...]
# 	[-chgrp [-R] GROUP PATH...]
# 	[-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
# 	[-chown [-R] [OWNER][:[GROUP]] PATH...]
# 	[-copyFromLocal [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>]
# 	[-copyToLocal [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
# 	[-count [-q] [-h] [-v] [-t [<storage type>]] [-u] [-x] [-e] <path> ...]
# 	[-cp [-f] [-p | -p[topax]] [-d] <src> ... <dst>]
# 	[-createSnapshot <snapshotDir> [<snapshotName>]]
# 	[-deleteSnapshot <snapshotDir> <snapshotName>]
# 	[-df [-h] [<path> ...]]
# 	[-du [-s] [-h] [-v] [-x] <path> ...]
# 	[-expunge]
# 	[-find <path> ... <expression> ...]
# 	[-get [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>]
# 	[-getfacl [-R] <path>]
# 	[-getfattr [-R] {-n name | -d} [-e en] <path>]
# 	[-getmerge [-nl] [-skip-empty-file] <src> <localdst>]
# 	[-head <file>]
# 	[-help [cmd ...]]
# 	[-ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] [<path> ...]]
# 	[-mkdir [-p] <path> ...]
# 	[-moveFromLocal <localsrc> ... <dst>]
# 	[-moveToLocal <src> <localdst>]
# 	[-mv <src> ... <dst>]
# 	[-put [-f] [-p] [-l] [-d] <localsrc> ... <dst>]
# 	[-renameSnapshot <snapshotDir> <oldName> <newName>]
# 	[-rm [-f] [-r|-R] [-skipTrash] [-safely] <src> ...]
# 	[-rmdir [--ignore-fail-on-non-empty] <dir> ...]
# 	[-setfacl [-R] [{-b|-k} {-m|-x <acl_spec>} <path>]|[--set <acl_spec> <path>]]
# 	[-setfattr {-n name [-v value] | -x name} <path>]
# 	[-setrep [-R] [-w] <rep> <path> ...]
# 	[-stat [format] <path> ...]
# 	[-tail [-f] [-s <sleep interval>] <file>]
# 	[-test -[defsz] <path>]
# 	[-text [-ignoreCrc] <src> ...]
# 	[-touch [-a] [-m] [-t TIMESTAMP ] [-c] <path> ...]
# 	[-touchz <path> ...]
# 	[-truncate [-w] <length> <path> ...]
# 	[-usage [cmd ...]]
