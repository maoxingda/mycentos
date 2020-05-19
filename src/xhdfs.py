#! /bin/python3
import os
import re
import readline
import sys
from subprocess import call, check_output


def logcall(acmd):
    if mode == 2:
        if enable_log:
            print(acmd)
        call(acmd, shell=True)
    elif mode == 1:
        if enable_log:
            print(f'>>> hdfs dfs -{acmd}')
        call(f'hdfs dfs -{acmd}', shell=True)


def unkown_cmd(acmd):
    print(f'unkown command ---> {acmd}')


def put():
    global cmd
    cmd = cmd[4:]

    if cmd[0] == '/':
        idx = cmd.find(' ')
        if idx != -1:
            if cmd[idx + 1] == '/':
                logcall(f'put {cmd}')
            else:
                logcall(f'put {cmd[:idx]} {xpath.join(cmd[idx + 1:])}')
        else:
            logcall(f'put {cmd} {remote_path.cwd}')
    else:
        idx = cmd.find(' ')
        if idx != -1:
            if cmd[idx + 1] == '/':
                logcall(f'put {xpath.join(cmd[:idx])} {cmd[idx + 1:]}')
            else:
                logcall(f'put {xpath.join(cmd[:idx])} {xpath.join(cmd[idx + 1:])}')
        else:
            logcall(f'put {xpath.join(cmd)} {remote_path.cwd}')


class Wacher:

    def subcribe(self, msg):
        pass


class PathWacherCompleter(Wacher):

    def __init__(self):
        readline.set_completer(PathWacherCompleter.completer)
        readline.parse_and_bind("tab: complete")

        self.__remote_choices = check_output([
            'hdfs', 'dfs', '-ls', '-C', '/']).decode('utf-8').split('\n')[:-1]
        self.__remote_choices = [rch.split('/')[-1] for rch in self.__remote_choices]
        self.__local_choices = check_output(['ls', os.getcwd()]).decode('utf-8').split('\n')[:-1]

    @staticmethod
    def completer(text, state):
        try:
            return [ch for ch in ['exit'] + wacher.choices() if ch.startswith(text)][state]
        except IndexError:
            return None

    def mk_local_choices(self, path):
        self.__local_choices = check_output(['ls', path]).decode('utf-8').split('\n')[:-1]

    def mk_remote_choices(self, path):
        self.__remote_choices = check_output(['hdfs', 'dfs', '-ls', '-C', path]).decode('utf-8').split('\n')[:-1]
        self.__remote_choices = [rch.split('/')[-1] for rch in self.__remote_choices]

    def choices(self):
        return self.__local_choices + self.__remote_choices

    def subcribe(self, msg):
        if mode == 1:
            self.mk_remote_choices(msg)
        elif mode == 2:
            self.mk_local_choices(msg)

    def choice_layout(self):
        if enable_log:
            if len(self.__local_choices) > 0:
                print('\n=============local choices=============')
            for choice in self.__local_choices:
                print(choice, end=' ')

            if len(self.__remote_choices) > 0:
                print('\n\n=============remote choices============')
            for choice in self.__remote_choices:
                print(choice, end=' ')

            if len(self.__local_choices) > 0 or len(self.__remote_choices) > 0:
                print()
        # print(readline.get_completer_delims())


class BasePath:

    def __init__(self, cwd, oldpwd):
        self.__init = True
        self.cwd = cwd
        self.oldpwd = oldpwd
        self.__wachers = []
        self.__init = False

    def add_wacher(self, awacher):
        self.__wachers.append(awacher)

    def dscribe(self, path):
        for iwacher in self.__wachers:
            iwacher.subcribe(path)

    @property
    def cwd(self):
        return self.__cwd

    @cwd.setter
    def cwd(self, val):
        if not self.__init and self.__cwd != val:
            self.dscribe(val)
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

    def join(self, path):
        return os.path.join(self.cwd, path)

    @staticmethod
    def ischdircmd(acmd):
        if re.fullmatch(r'\s*\.\s*', acmd) or \
                re.fullmatch(r'\s*\.\.\s*', acmd) or \
                re.fullmatch(r'\s*cd\s*', acmd) or \
                re.fullmatch(r'\s*cd\s+-\s*', acmd) or \
                re.fullmatch(r'\s*cd\s*'
                             r'(?P<root>/?)'
                             r'(?P<arg>\w+(?:/\w+)*)/?\s*', acmd):
            return True
        else:
            return False

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


class LocalPath(BasePath):

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


class RemotePath(BasePath):

    def __init__(self, cwd, oldpwd):
        super().__init__(cwd, oldpwd)

    def cdhome(self):
        self.cwd = '/'

    @staticmethod
    def ismkdir():
        return re.fullmatch(r'\s*md\s+'
                            r'(?P<option>(?:-p\s+)?)'
                            r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', cmd)

    @staticmethod
    def mkdir():
        what = re.fullmatch(r'\s*md\s+'
                            r'(?P<option>(?:-p\s+)?)'
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
                    logcall(f'mkdir -p {remote_path.join(arg)}')
                else:
                    logcall(f'mkdir {remote_path.join(arg)}')

    @staticmethod
    def isrmdir():
        return re.fullmatch(r'^\s*rm\s+'
                            r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', cmd)

    @staticmethod
    def rmdir():
        what = re.fullmatch(r'^\s*rm\s+'
                            r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', cmd)

        if what:
            root = what.group('root')
            arg = what.group('prefix') + what.group('suffix')

            if root:
                logcall(f'rm -r {arg}')
            else:
                logcall(f'rm -r {remote_path.join(arg)}')

        # wildcard
        elif re.fullmatch(r'^\s*rm\s+\*\.\w+', cmd):
            logcall(cmd)


class Path:

    def __init__(self, lpath: BasePath, rpath: BasePath):
        self.__local_path: BasePath = lpath
        self.__remote_path: BasePath = rpath

    def add_wacher(self, lwacher: Wacher, rwacher: Wacher):
        self.__local_path.add_wacher(lwacher)
        self.__remote_path.add_wacher(rwacher)

    def dir_layout(self):
        local_bname = os.path.basename(self.__local_path.cwd)
        if local_bname == '':
            local_bname = '/'

        remote_bname = os.path.basename(self.__remote_path.cwd)
        if remote_bname == '':
            remote_bname = '/'

        pmode = 'hdfs'
        prompt = '>'
        if mode == 2:
            prompt += '>>'
            pmode = 'local'

        return f'\n[{pmode}][{local_bname}:{remote_bname}]{prompt} '

    def join(self, path):
        if mode == 1:
            self.__local_path.join(path)
        elif mode == 2:
            self.__remote_path.join(path)


if __name__ == '__main__':

    mode = 1
    enable_log = len(sys.argv) > 1 and sys.argv[1] == '1'

    wacher = PathWacherCompleter()

    local_path = LocalPath(os.getcwd(), os.getcwd())
    remote_path = RemotePath('/', '/')

    local_path.add_wacher(wacher)
    remote_path.add_wacher(wacher)

    xpath = Path(local_path, remote_path)

    while True:

        wacher.choice_layout()
        cmd = input(xpath.dir_layout())

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
                if BasePath.ischdircmd(cmd):
                    local_path.chdir(cmd)
                # others
                else:
                    logcall(cmd)

            # hdfs commands
            else:

                # change dir
                if cmd == 'pwd':
                    print(remote_path.cwd)
                elif BasePath.ischdircmd(cmd):
                    remote_path.chdir(cmd)

                # create dir
                elif remote_path.ismkdir():
                    remote_path.mkdir()

                # touch file
                elif cmd.startswith('touch '):
                    pass

                # del dir or file
                elif remote_path.isrmdir():
                    remote_path.rmdir()

                # list dir content
                elif cmd == 'ls':
                    logcall(f'ls {remote_path.cwd}')
                elif cmd == 'lsc':
                    logcall(f'ls -C {remote_path.cwd}')
                elif cmd.startswith('ls '):
                    if cmd[3] == '/':
                        logcall(f'ls {cmd[3:]}')
                    else:
                        logcall(f'ls {remote_path.join(cmd[3:])}')

                # cat file content
                elif cmd.startswith('cat '):
                    cmd = cmd[4:]
                    if cmd[0] == '/':
                        logcall(f'cat {cmd}')
                    else:
                        logcall(f'cat {remote_path.join(cmd)}')

                # put or get file
                elif cmd.startswith('put '):
                    put()
                elif cmd.startswith('get '):
                    cmd = cmd[4:]
                    if cmd[0] == '/':
                        logcall(f'get {cmd}')
                    else:
                        logcall(f'get {remote_path.join(cmd)}')

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
