#! /bin/python3
import os
import re
import readline
import sys
from enum import Enum, unique
from subprocess import call, check_output


class PathWacher:

    def pathchange(self, apath):
        pass


class PathWacherCompleter(PathWacher):

    def __init__(self):
        readline.set_completer(PathWacherCompleter.completer)
        readline.parse_and_bind("tab: complete")

        self.__remote_choices = check_output([
            'hdfs', 'dfs', '-ls', '-C', '/']).decode('utf-8').split('\n')[:-1]
        self.__remote_choices = [os.path.basename(d) for d in self.__remote_choices]
        self.__local_choices = check_output(['ls', os.getcwd()]).decode('utf-8').split('\n')[:-1]

    @staticmethod
    def completer(text, state):
        try:
            return [ch for ch in ['exit',
                                  'log_enable',
                                  'cmd_enable'] + app.wacher().choices() if ch.startswith(text)][state]
        except IndexError:
            return None

    def mk_local_choices(self, apath):
        self.__local_choices = check_output(['ls', apath]).decode('utf-8').split('\n')[:-1]

    def mk_remote_choices(self, apath):
        self.__remote_choices = check_output(['hdfs', 'dfs', '-ls', '-C', apath]).decode('utf-8').split('\n')[:-1]
        self.__remote_choices = [rch.split('/')[-1] for rch in self.__remote_choices]

    def choices(self):
        return self.__local_choices + self.__remote_choices

    def pathchange(self, apath):
        if app.mode:
            self.mk_remote_choices(apath)
        else:
            self.mk_local_choices(apath)

    def choice_layout(self):
        if app.enable_log:
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

    def dscribe(self, apath):
        for iwacher in self.__wachers:
            iwacher.pathchange(apath)

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

    def cd(self, apath):
        pass

    def cdpardir(self):
        pass

    def cdhome(self):
        pass

    def cdoldpwd(self):
        pass

    def join(self, apath):
        return os.path.join(self.cwd, apath)

    def chdir(self, cmd_elem):
        head, *tail = cmd_elem
        # .
        if head == '.':
            self.oldpwd = self.cwd
            self.cwd = os.path.dirname(self.cwd)

        # ..
        elif head == '..':
            self.oldpwd = self.cwd
            self.cwd = os.path.dirname(self.cwd)
            self.cwd = os.path.dirname(self.cwd)

        # cd
        elif head == 'cd':
            if len(tail) == 0:
                self.oldpwd = self.cwd
                self.cdhome()

            # cd -
            elif tail[0] == '-':
                temp = self.oldpwd
                self.oldpwd = self.cwd
                self.cwd = temp

            # others
            else:
                self.oldpwd = self.cwd
                self.cwd = self.join(tail[0])

        # unreachable
        else:
            CmdHelper.unkown_cmd(cmd_elem)

    def mkdir(self, apath):
        pass

    def rmdir(self, apath):
        pass


class Path:

    def __init__(self):
        self.__local_path = Path.LocalPath(os.getcwd(), os.getcwd())
        self.__remote_path = Path.RemotePath('/', '/')
        self.__path = self.__remote_path

    class LocalPath(BasePath):

        def __init__(self, cwd, oldpwd):
            super().__init__(cwd, oldpwd)

        def cd(self, apath):
            os.chdir(apath)

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

        def mkdir(self, apath):
            what = re.fullmatch(r'\s*md\s+'
                                r'(?P<option>(?:-p\s+)?)'
                                r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', apath)
            if what:
                option = what.group('option')
                root = what.group('root')
                arg = what.group('prefix') + what.group('suffix')
                if root:
                    if option:
                        CmdHelper.logcall(f'mkdir -p {arg}')
                    else:
                        CmdHelper.logcall(f'mkdir {arg}')
                else:
                    if option:
                        CmdHelper.logcall(f'mkdir -p {self.join(arg)}')
                    else:
                        CmdHelper.logcall(f'mkdir {self.join(arg)}')

        def rmdir(self, apath):
            what = re.fullmatch(r'^\s*rm\s+'
                                r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', apath)

            if what:
                root = what.group('root')
                arg = what.group('prefix') + what.group('suffix')

                if root:
                    CmdHelper.logcall(f'rm -r {arg}')
                else:
                    CmdHelper.logcall(f'rm -r {self.join(arg)}')

            # wildcard
            elif re.fullmatch(r'^\s*rm\s+\*\.\w+', apath):
                CmdHelper.logcall(apath)

    def mode(self, mode):
        if mode:
            self.__path = self.__remote_path
        else:
            self.__path = self.__local_path

    def add_wacher(self, awacher):
        self.__local_path.add_wacher(awacher)
        self.__remote_path.add_wacher(awacher)

    def prompt(self):
        local_bname = os.path.basename(self.__local_path.cwd)
        if local_bname == '':
            local_bname = '/'

        remote_bname = os.path.basename(self.__remote_path.cwd)
        if remote_bname == '':
            remote_bname = '/'

        prompt = '>'
        cmd_mode_prompt = 'hdfs'

        if not app.mode:
            prompt += '>>'
            cmd_mode_prompt = 'local'

        return f'\n[{cmd_mode_prompt}][{local_bname}:{remote_bname}]{prompt} '

    def join(self, apath):
        return self.__path.join(apath)

    def chdir(self, apath):
        self.__path.chdir(apath)

    def cwd(self):
        return self.__path.cwd

    def dcwd(self):
        return self.__local_path.cwd, self.__remote_path.cwd

    def mkdir(self, apath):
        self.__path.mkdir(apath)

    def rmdir(self, apath):
        if app.mode == CmdMode.hdfs:
            self.__remote_path.rmdir(apath)
        elif app.mode == CmdMode.local:
            pass

    def pwd(self):
        print(self.cwd())

    def lsdir(self, apath):
        what = re.split(r'\s+', apath)
        length = len(what)
        if length == 1:
            what.append(self.cwd())
        else:
            if what[-1].startswith('-'):
                what.append(self.cwd())
            else:
                what[-1] = self.join(what[-1])
        CmdHelper.logcall(' '.join(what))


class CmdHelper:
    # pattern
    pat_lsdir = r'\s*ls\s*' \
                r'(?P<option>(-\w\s+)?)' \
                r'(?P<root>/?)(?P<arg>(?:\w+)?(?:/\w+)*)/?\s*'

    @staticmethod
    def unkown_cmd(acmd):
        print(f'unkown command ---> {acmd}')

    @staticmethod
    def logcall(acmd):
        if not app.mode:
            if app.enable_cmd:
                print(acmd)
            call(acmd, shell=True)
        else:
            if app.enable_cmd:
                print(f'>>> hdfs dfs -{acmd}')
            call(f'hdfs dfs -{acmd}', shell=True)

    @staticmethod
    def __ischdircmd(acmd):
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

    @staticmethod
    def __isrmdir(acmd):
        return re.fullmatch(r'^\s*rm\s+'
                            r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', acmd)

    @staticmethod
    def __ismkdir(acmd):
        return re.fullmatch(r'\s*md\s+'
                            r'(?P<option>(?:-p\s+)?)'
                            r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', acmd)

    @staticmethod
    def __islsdir(acmd):
        return re.match(r'\s*ls', acmd)
        # return re.fullmatch(CmdHelper.pat_lsdir, acmd)

    @staticmethod
    def cmdtype(acmd):
        what = re.split(r'\s+', acmd)
        what = [e for e in what if e != '']
        length = len(what)
        if length > 0:
            if what[0] == '.' or what[0] == '..' or what[0] == 'cd':
                app.path().chdir(what)
            elif what[0] == 'pwd':
                print(app.path().cwd())
            elif what[0] == 'exit':
                sys.exit()
            elif what[0] == '!':
                app.mode = not app.mode
            # if length == 1:
            #     what.append(app.path().cwd())
            # else:
            #     if what[-1].startswith('-'):
            #         what.append(app.path().cwd())
            #     else:
            #         what[-1] = app.path().join(what[-1])
            # CmdHelper.logcall(' '.join(what))

        # if CmdHelper.__ischdircmd(acmd):
        #     return CmdType.chdir
        # elif CmdHelper.__ismkdir(acmd):
        #     return CmdType.mkdir
        # elif CmdHelper.__isrmdir(acmd):
        #     return CmdType.rmdir
        # elif CmdHelper.__islsdir(acmd):
        #     return CmdType.lsdir
        # elif re.fullmatch(r'\s*exit\s*', acmd):
        #     return CmdType.exit
        # elif re.fullmatch(r'\s*pwd\s*', acmd):
        #     return CmdType.pwd
        # elif re.fullmatch(r'\s*!\s*', acmd):
        #     return CmdType.chmode
        # elif re.match(r'\s*cat\s+\S', acmd):
        #     return CmdType.cat
        # else:
        return CmdType.other

    @staticmethod
    def chdir(acmd):
        app.path().chdir(acmd)

    @staticmethod
    def mkdir(acmd):
        app.path().mkdir(acmd)

    @staticmethod
    def rmdir(acmd):
        app.path().rmdir(acmd)

    @staticmethod
    def put(acmd):
        acmd = acmd[4:]

        if acmd[0] == '/':
            idx = acmd.find(' ')
            if idx != -1:
                if acmd[idx + 1] == '/':
                    CmdHelper.logcall(f'put {acmd}')
                else:
                    CmdHelper.logcall(f'put {acmd[:idx]} {app.path().join(acmd[idx + 1:])}')
            else:
                CmdHelper.logcall(f'put {acmd} {app.path().cwd}')
        else:
            idx = acmd.find(' ')
            if idx != -1:
                if acmd[idx + 1] == '/':
                    CmdHelper.logcall(f'put {app.path().join(acmd[:idx])} {acmd[idx + 1:]}')
                else:
                    CmdHelper.logcall(f'put {app.path().join(acmd[:idx])} {app.path().join(acmd[idx + 1:])}')
            else:
                CmdHelper.logcall(f'put {app.path().join(acmd)} {app.path().cwd}')

    @staticmethod
    def exit():
        app.exit()

    @staticmethod
    def chmode():
        app.chmode()

    @staticmethod
    def pwd():
        app.path().pwd()

    @staticmethod
    def lsdir(acmd):
        app.path().lsdir(acmd)

    @staticmethod
    def cat(acmd):
        CmdHelper.logcall(acmd)

    @staticmethod
    def parse(acmd):
        what = re.split(r'\s+', acmd)
        what = [e for e in what if e != '']
        length = len(what)
        if length > 0:
            if what[0] == '!':
                app.mode = not app.mode
            elif what[0] == 'pwd':
                print(app.path().cwd())
            elif what[0] == 'exit':
                sys.exit()
            elif what[0] == 'log_enable':
                app.enable_log = not app.enable_log
            elif what[0] == 'cmd_enable':
                app.enable_cmd = not app.enable_cmd
            elif what[0] == 'usage':
                if length > 1 and re.fullmatch(r'\w+', what[1]):
                    CmdHelper.logcall(f'{what[0]} {what[1]}')
            elif what[0] == '.' or what[0] == '..' or what[0] == 'cd':
                app.path().chdir(what)
            elif what[0] == 'put':
                loc, rem = app.path().dcwd()
                if length > 2:
                    CmdHelper.logcall(f'put {os.path.join(loc, what[1])} {os.path.join(rem, what[2])}')
                elif length > 1:
                    CmdHelper.logcall(f'put {os.path.join(loc, what[1])} {rem}')
            elif what[0] == 'get':
                loc, rem = app.path().dcwd()
                if length > 2:
                    CmdHelper.logcall(f'get {os.path.join(rem, what[1])} {os.path.join(loc, what[2])}')
                elif length > 1:
                    CmdHelper.logcall(f'get {os.path.join(rem, what[1])} {loc}')
            else:
                command, *options = what
                if re.fullmatch(r'\w+', command):
                    for i, e in enumerate(options):
                        if not e.startswith('-'):
                            options[i] = app.path().join(e)
                    if len(options) == 0 or options[-1].startswith('-'):
                        if command != 'rm':
                            options.append(app.path().cwd())
                    CmdHelper.logcall(' '.join([command] + options))
                    # print(' '.join([command] + options))


@unique
class CmdMode(Enum):
    hdfs = 1
    local = 2


@unique
class CmdType(Enum):
    chdir = 1
    mkdir = 2
    rmdir = 3
    pwd = 4
    lsdir = 5
    cat = 6

    chmode = 253
    exit = 254
    other = 255


class Main:

    def main(self, args, argv):

        self.init(args, argv)

        while True:
            self.wacher().choice_layout()

            cmd = input(self.path().prompt())

            CmdHelper.parse(cmd)

    def __init__(self):
        self.__enable_log = False
        self.__enable_cmd = False
        self.__hdfs = True

        self.__path = Path()
        self.__wacher = PathWacherCompleter()
        self.__path.add_wacher(self.__wacher)

    def init(self, args, argv):
        self.__enable_log = args > 1 and argv[1] == '1'

    @property
    def mode(self):
        return self.__hdfs

    @mode.setter
    def mode(self, val):
        self.__hdfs = val
        self.path().mode(val)

    def path(self):
        return self.__path

    def wacher(self):
        return self.__wacher

    @property
    def enable_log(self):
        return self.__enable_log

    @enable_log.setter
    def enable_log(self, val):
        self.__enable_log = val

    @property
    def enable_cmd(self):
        return self.__enable_cmd

    @enable_cmd.setter
    def enable_cmd(self, val):
        self.__enable_cmd = val

    def exit(self):
        if self.mode == CmdMode.local:
            self.mode = CmdMode.hdfs
        elif self.mode == CmdMode.hdfs:
            sys.exit()


if __name__ == '__main__':
    try:
        app = Main()

        app.main(len(sys.argv), sys.argv)
    except Exception as e:
        print(e)

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
