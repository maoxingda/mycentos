#! /bin/python3
import atexit
import os
import re
import readline
import sys
from enum import Enum, unique
from subprocess import call, check_output


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

class PathWacher:

    def pathchange(self, apath):
        pass


class PathWacherCompleter(PathWacher):

    def __init__(self):
        readline.parse_and_bind("tab: complete")
        readline.set_completer(PathWacherCompleter.completer)
        self.__histfile = os.path.expandvars('${HOME}/.xhdfs_history')
        if os.path.exists(self.__histfile):
            readline.read_history_file(self.__histfile)
        atexit.register(self.save_histfile)

        self.__remote_choices = check_output([
            'hdfs', 'dfs', '-ls', '-C', '/']).decode('utf-8').split('\n')[:-1]
        self.__remote_choices = [os.path.basename(d) for d in self.__remote_choices]
        self.__local_choices = check_output(['ls', os.getcwd()]).decode('utf-8').split('\n')[:-1]

    @staticmethod
    def completer(text, state):
        try:
            lchoices = []
            rchoices = []

            line = readline.get_line_buffer()
            line = re.split(r'\s+', line)[-1]

            if line.find('/') != -1:

                line = os.path.dirname(line)
                path = os.path.join(os.getcwd(), line)

                if os.path.exists(path):
                    lchoices = check_output(['ls', path]).decode('utf-8').split('\n')[:-1]
                else:
                    path = os.path.join(app.path().cwd(), line)

                    rchoices = check_output(['hdfs', 'dfs', '-ls', '-C', path]).decode('utf-8').split('\n')[:-1]
                    rchoices = [rch.split('/')[-1] for rch in rchoices]

            results = [ch for ch in CmdHelper.hdfscmd + lchoices + rchoices
                       + app.wacher().choices() if ch.startswith(text)]
            return results[state]
        except IndexError:
            return None

    def save_histfile(self):
        readline.write_history_file(self.__histfile)

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
                print('\n--->local')
            for choice in self.__local_choices:
                print(choice, end=' ')

            if len(self.__remote_choices) > 0:
                print('\n\n--->hdfs')
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

    def cd(self, path):
        # cd
        if len(path) == 1:
            self.oldpwd = self.cwd
            self.cdhome()

        # cd -
        elif path[1] == '-':
            temp = self.oldpwd
            self.oldpwd = self.cwd
            self.cwd = temp

            temp = self.oldpwd
            os.chdir(self.oldpwd)
            self.oldpwd = os.getcwd()
            self.cwd = temp

            temp

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

        def cd(self, path):
            if path[0] == '.':
                self.oldpwd = self.cwd
                os.chdir(os.pardir)
                self.cwd = os.getcwd()
            elif path[0] == '..':
                self.oldpwd = self.cwd
                os.chdir(os.pardir)
                os.chdir(os.pardir)
                self.cwd = os.getcwd()
            elif len(path) == 1:
                self.oldpwd = self.cwd
                os.chdir(os.path.expandvars('${HOME}'))
                self.cwd = os.getcwd()
            else:
                dst = ''
                if path[1] == '-':
                    dst = self.oldpwd
                else:
                    dst = os.path.join(self.cwd, path[1])
                if os.path.exists(dst):
                    os.chdir(dst)
                    self.oldpwd = self.cwd
                    self.cwd = os.getcwd()
                else:
                    print(f'{dst} not find or is a file')

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

        def cd(self, path):
            if path[0] == '.':
                self.oldpwd = self.cwd
                self.cwd = os.path.dirname(self.cwd)
            elif path[0] == '..':
                self.oldpwd = self.cwd
                self.cwd = os.path.dirname(self.cwd)
                self.cwd = os.path.dirname(self.cwd)
            elif len(path) == 1:
                self.oldpwd = self.cwd
                self.cwd = '/'
            else:
                dst = ''
                if path[1] == '-':
                    dst = self.oldpwd
                else:
                    dst = os.path.join(self.cwd, path[1])

                if 0 == call(['hadoop', 'fs', '-test', '-d', dst]):
                    self.oldpwd = self.cwd
                    self.cwd = dst
                else:
                    print(f'{dst} not find or is a file')

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

    def cd(self, path):
        self.__path.cd(path)


class CmdHelper:
    # support cmds
    hdfscmd = ['!',
               'pwd',
               'exit',
               'cmd_enable',
               'log_enable',
               'appendToFile',
               'cat',
               'copyFromLocal',
               'copyToLocal',
               'get',
               'head',
               'help',
               'ls',
               'mkdir',
               'moveFromLocal',
               'moveToLocal',
               'mv',
               'put',
               'rm',
               'tail',
               'touch',
               'truncate'
               ]

    # pattern
    pat_lsdir = r'\s*ls\s*' \
                r'(?P<option>(-\w\s+)?)' \
                r'(?P<root>/?)(?P<arg>(?:\w+)?(?:/\w+)*)/?\s*'

    # pattern
    pat_chdir = r'\s*cd\s*' \
                r'(?P<option>(-\w\s+)?)' \
                r'(?P<root>/?)(?P<arg>(?:\w+)?(?:/\w+)*)/?\s*'

    # pattern
    pat_mkdir = r'\s*md\s*' \
                r'(?P<option>(-\w\s+)?)' \
                r'(?P<root>/?)(?P<arg>(?:\w+)?(?:/\w+)*)/?\s*'

    # pattern
    pat_rmdir = r'\s*rm\s*' \
                r'(?P<option>(-\w\s+)?)' \
                r'(?P<root>/?)(?P<arg>(?:\w+)?(?:/\w+)*)/?\s*'

    @staticmethod
    def unkown_cmd(cmd):
        print(f'unkown command ---> {cmd}')

    @staticmethod
    def logcall(cmd):
        if not app.mode:
            if len(cmd) > 0:
                cmd = ' '.join(cmd)
            if app.enable_cmd:
                print(cmd)
            call(cmd, shell=True)
        else:
            cmd = ' '.join([f'hadoop fs -{cmd[0]}'] + cmd[1:])
            if app.enable_cmd:
                print(f'---> {cmd}')
            call(cmd, shell=True)

    @staticmethod
    def __ischdircmd(cmd):
        if re.fullmatch(r'\s*\.\s*', cmd) or \
                re.fullmatch(r'\s*\.\.\s*', cmd) or \
                re.fullmatch(r'\s*cd\s*', cmd) or \
                re.fullmatch(r'\s*cd\s+-\s*', cmd) or \
                re.fullmatch(r'\s*cd\s*'
                             r'(?P<root>/?)'
                             r'(?P<arg>\w+(?:/\w+)*)/?\s*', cmd):
            return True
        else:
            return False

    @staticmethod
    def __isrmdir(cmd):
        return re.fullmatch(r'^\s*rm\s+'
                            r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', cmd)

    @staticmethod
    def __ismkdir(cmd):
        return re.fullmatch(r'\s*md\s+'
                            r'(?P<option>(?:-p\s+)?)'
                            r'(?P<root>/?)(?P<prefix>\w+)(?P<suffix>(/\w+)*)', cmd)

    @staticmethod
    def __islsdir(cmd):
        return re.match(r'\s*ls', cmd)
        # return re.fullmatch(CmdHelper.pat_lsdir, cmd)

    @staticmethod
    def cmdtype(cmd):
        what = re.split(r'\s+', cmd)
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

        # if CmdHelper.__ischdircmd(cmd):
        #     return CmdType.chdir
        # elif CmdHelper.__ismkdir(cmd):
        #     return CmdType.mkdir
        # elif CmdHelper.__isrmdir(cmd):
        #     return CmdType.rmdir
        # elif CmdHelper.__islsdir(cmd):
        #     return CmdType.lsdir
        # elif re.fullmatch(r'\s*exit\s*', cmd):
        #     return CmdType.exit
        # elif re.fullmatch(r'\s*pwd\s*', cmd):
        #     return CmdType.pwd
        # elif re.fullmatch(r'\s*!\s*', cmd):
        #     return CmdType.chmode
        # elif re.match(r'\s*cat\s+\S', cmd):
        #     return CmdType.cat
        # else:
        return CmdType.other

    @staticmethod
    def chdir(cmd):
        app.path().chdir(cmd)

    @staticmethod
    def rmdir(cmd):
        app.path().rmdir(cmd)

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
    def lsdir(cmd):
        app.path().lsdir(cmd)

    @staticmethod
    def parse(cmd):
        cmd = re.split(r'\s+', cmd)
        cmd = [ele for ele in cmd if ele != '']

        if not len(cmd):
            return

        cname = cmd[0]

        if cname == '!':
            app.mode = not app.mode
        elif cname == 'pwd':
            print(app.path().cwd())
        elif cname == 'exit':
            sys.exit()
        elif cname == 'log_enable':
            app.enable_log = not app.enable_log
        elif cname == 'cmd_enable':
            app.enable_cmd = not app.enable_cmd

        # impl separately because there are two file systems (local and hdfs)
        elif cname == '.' or cname == '..' or cname == 'cd':
            app.path().cd(cmd)

        # system commands
        elif not app.mode:
            return CmdHelper.logcall(cmd)

        # hdfs shell commands
        # hadoop fs -appendToFile <localsrc> ... <dst>
        elif cname == 'appendToFile':
            CmdHelper.appendToFile(cmd)
        # hadoop fs -cat [-ignoreCrc] <src> ...
        elif cname == 'cat':
            CmdHelper.cat(cmd)
        # hadoop fs -copyFromLocal <localsrc> URI
        elif cname == 'copyFromLocal':
            CmdHelper.copyFromLocal(cmd)
        # hadoop fs -copyToLocal [-ignorecrc] [-crc] URI <localdst>
        elif cname == 'copyToLocal':
            CmdHelper.copyToLocal(cmd)
        # hadoop fs -get [-ignorecrc] [-crc] [-p] [-f] <src> <localdst>
        elif cname == 'get':
            CmdHelper.get(cmd)
        # hadoop fs -head URI
        elif cname == 'head':
            CmdHelper.head(cmd)
        # hadoop fs -help
        elif cname == 'help':
            CmdHelper.help(cmd)
        # hadoop fs -ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] <args>
        elif cname == 'ls':
            CmdHelper.ls(cmd)
        # hadoop fs -mkdir [-p] <paths>
        elif cname == 'mkdir':
            CmdHelper.mkdir(cmd)
        # hadoop fs -moveFromLocal <localsrc> <dst>
        elif cname == 'moveFromLocal':
            CmdHelper.moveFromLocal(cmd)
        # hadoop fs -moveToLocal [-crc] <src> <dst>
        elif cname == 'moveToLocal':
            CmdHelper.moveToLocal(cmd)
        # hadoop fs -mv URI [URI ...] <dest>
        elif cname == 'mv':
            CmdHelper.mv(cmd)
        # hadoop fs -put [-f] [-p] [-l] [-d] [ - | <localsrc1> .. ]. <dst>
        elif cname == 'put':
            CmdHelper.put(cmd)
        # hadoop fs -rm [-f] [-r |-R] [-skipTrash] [-safely] URI [URI ...]
        elif cname == 'rm':
            CmdHelper.rm(cmd)
        # hadoop fs -tail [-f] URI
        elif cname == 'tail':
            CmdHelper.tail(cmd)
        # hadoop fs -touch [-a] [-m] [-t TIMESTAMP] [-c] URI [URI ...]
        elif cname == 'touch':
            CmdHelper.touch(cmd)
        # hadoop fs -truncate [-w] <length> <paths>
        elif cname == 'truncate':
            CmdHelper.truncate(cmd)

            # elif cmd[0] == '!':
            #     app.mode = not app.mode
            # elif cmd[0] == 'pwd':
            #     print(app.path().cwd())
            # elif cmd[0] == 'exit':
            #     sys.exit()
            # elif cmd[0] == 'log_enable':
            #     app.enable_log = not app.enable_log
            # elif cmd[0] == 'cmd_enable':
            #     app.enable_cmd = not app.enable_cmd
            # elif cmd[0] == 'usage':
            #     if length > 1 and re.fullmatch(r'\w+', cmd[1]):
            #         CmdHelper.logcall(f'{cmd[0]} {cmd[1]}')
            # elif cmd[0] == '.' or cmd[0] == '..' or cmd[0] == 'cd':
            #     app.path().chdir(cmd)
            # elif cmd[0] == 'put':
            #     loc, rem = app.path().dcwd()
            #     if length > 2:
            #         CmdHelper.logcall(f'put {os.path.join(loc, cmd[1])} {os.path.join(rem, cmd[2])}')
            #     elif length > 1:
            #         CmdHelper.logcall(f'put {os.path.join(loc, cmd[1])} {rem}')
            # elif cmd[0] == 'get':
            #     loc, rem = app.path().dcwd()
            #     if length > 2:
            #         CmdHelper.logcall(f'get {os.path.join(rem, cmd[1])} {os.path.join(loc, cmd[2])}')
            #     elif length > 1:
            #         CmdHelper.logcall(f'get {os.path.join(rem, cmd[1])} {loc}')
            # else:
            #     command, *options = cmd
            #     if re.fullmatch(r'\w+', command):
            #         for i, e in enumerate(options):
            #             if not e.startswith('-'):
            #                 options[i] = app.path().join(e)
            #         if len(options) == 0 or options[-1].startswith('-'):
            #             if command != 'rm':
            #                 options.append(app.path().cwd())
            #         CmdHelper.logcall(' '.join([command] + options))
            #         # print(' '.join([command] + options))

    @staticmethod
    def appendToFile(cmd):
        # hadoop fs -appendToFile <localsrc> ... <dst>
        if len(cmd) < 3:
            return

        read_from_stdin = [ele for ele in cmd[1:] if ele == '-']

        uri = r'hdfs://(?:\w+|(?:\d{1,3})(?:(\.\w+){1,3}))(/\w+)+'

        if read_from_stdin:
            # hadoop fs -appendToFile - hdfs://[host|ip]/hadoopfile
            if not cmd[2].startswith('hdfs://'):
                cmd[2] = app.path().join(cmd[2])

            CmdHelper.logcall(cmd)
        else:
            addr = [ele for ele in cmd[1:] if re.fullmatch(uri, ele)]
            if addr:
                # hadoop fs -appendToFile localfile ... hdfs://[host|ip]/hadoopfile
                lcwd, rcwd = app.path().dcwd()

                for i in range(1, len(cmd) - 1):
                    cmd[i] = os.path.join(lcwd, cmd[i])

                CmdHelper.logcall(cmd)
            else:
                # hadoop fs -appendToFile localfile ... /hadoopfile
                lcwd, rcwd = app.path().dcwd()

                for i in range(1, len(cmd) - 1):
                    cmd[i] = os.path.join(lcwd, cmd[i])

                cmd[-1] = os.path.join(rcwd, cmd[-1])

                CmdHelper.logcall(cmd)

    @staticmethod
    def cat(cmd):
        # hadoop fs -cat [-ignoreCrc] <src> ...
        if len(cmd) < 2 or cmd[-1].startswith('-'):
            return

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(cmd)):
            if not cmd[i].startswith('file://') and \
                    not cmd[i].startswith('hdfs://') and \
                    not cmd[i].startswith('/') and not cmd[i].startswith('-'):
                cmd[i] = os.path.join(rcwd, cmd[i])

        CmdHelper.logcall(cmd)

    @staticmethod
    def copyFromLocal(cmd):
        # hadoop fs -copyFromLocal <localsrc> URI
        if len(cmd) < 3:
            return

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(cmd) - 1):
            cmd[i] = os.path.join(lcwd, cmd[i])

        if not cmd[-1].startswith('hdfs://'):
            cmd[-1] = os.path.join(rcwd, cmd[-1])

        CmdHelper.logcall(cmd)

    @staticmethod
    def copyToLocal(cmd):
        # hadoop fs -copyToLocal [-ignorecrc] [-crc] URI <localdst>
        if len(cmd) < 3 or cmd[-1].startswith('-'):
            return

        lcwd, rcwd = app.path().dcwd()
        for i in range(1, len(cmd) - 1):
            if not cmd[i].startswith('hdfs://') and not cmd[i].startswith('-'):
                cmd[i] = os.path.join(rcwd, cmd[i])

        cmd[-1] = os.path.join(lcwd, cmd[-1])

        CmdHelper.logcall(cmd)

    @staticmethod
    def get(cmd):
        # hadoop fs -get [-ignorecrc] [-crc] [-p] [-f] <src> <localdst>
        if len(cmd) == 1 or cmd[-1].startswith('-'):
            return

        lcwd, rcwd = app.path().dcwd()

        if len(cmd) == 2:
            cmd.append(lcwd)

        for i in range(1, len(cmd) - 1):
            if not cmd[i].startswith('hdfs://') and not cmd[i].startswith('-'):
                cmd[i] = os.path.join(rcwd, cmd[i])

        cmd[-1] = os.path.join(lcwd, cmd[-1])

        CmdHelper.logcall(cmd)

    @staticmethod
    def head(cmd):
        # hadoop fs -head URI
        if len(cmd) < 2:
            return

        for i in range(1, len(cmd)):
            if not cmd[i].startswith('hdfs://'):
                cmd[i] = app.path().join(cmd[i])

        CmdHelper.logcall(cmd)

    @staticmethod
    def ls(cmd):
        # hadoop fs -ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] <args>
        if len(cmd) == 1 or cmd[-1].startswith('-'):
            cmd.append(app.path().cwd())

            CmdHelper.logcall(cmd)
        else:
            for i in range(1, len(cmd)):
                if not cmd[i].startswith('-'):
                    cmd[i] = app.path().join(cmd[i])

            CmdHelper.logcall(cmd)

    @staticmethod
    def mkdir(cmd):

        # hadoop fs -mkdir [-p] <paths>
        if len(cmd) < 2 or cmd[-1].startswith('-'):
            return

        for i in range(1, len(cmd)):
            if not cmd[i].startswith('hdfs://') and not cmd[i].startswith('-'):
                cmd[i] = app.path().join(cmd[i])

        CmdHelper.logcall(cmd)

    @staticmethod
    def moveFromLocal(cmd):
        # hadoop fs -moveFromLocal <localsrc> <dst>

        lcwd, rcwd = app.path().dcwd()

        if len(cmd) == 2:
            cmd.append(rcwd)
            cmd[-1] = os.path.join(lcwd, cmd[-1])

            CmdHelper.logcall(cmd)
        else:
            for i in range(1, len(cmd) - 1):
                cmd[i] = os.path.join(lcwd, cmd[i])
            cmd[-1] = os.path.join(rcwd, cmd[-1])

            CmdHelper.logcall(cmd)

    @staticmethod
    def moveToLocal(cmd):

        # hadoop fs -moveToLocal [-crc] <src> <dst>
        if len(cmd) < 3:
            return

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(cmd) - 1):
            if not cmd[i].startswith('hdfs://') and not cmd[i].startswith('-'):
                cmd[i] = os.path.join(rcwd, cmd[i])

        cmd[-1] = os.path.join(lcwd, cmd[-1])

        CmdHelper.logcall(cmd)

    @staticmethod
    def mv(cmd):
        # hadoop fs -mv URI [URI ...] <dest>
        if len(cmd) < 3:
            return

        for i in range(1, len(cmd)):
            cmd[i] = app.path().join(cmd[i])

        CmdHelper.logcall(cmd)

    @staticmethod
    def put(cmd):

        # hadoop fs -put [-f] [-p] [-l] [-d] [ - | <localsrc1> .. ]. <dst>

        if len(cmd) == 1 or cmd[-1].startswith('-'):
            return

        lcwd, rcwd = app.path().dcwd()

        if len(cmd) == 2:
            cmd.append(rcwd)

        read_from_stdin = [ele for ele in cmd[1:] if ele == '-']

        uri = r'hdfs://(?:\w+|(?:\d{1,3})(?:(\.\w+){1,3}))(/\w+)+'

        if read_from_stdin:
            # hadoop fs -put - hdfs://[host|ip]/hadoopfile
            if not cmd[2].startswith('hdfs://'):
                app.path().join(cmd[2])

            CmdHelper.logcall(cmd)
        else:
            addr = [ele for ele in cmd[1:] if re.fullmatch(uri, ele)]
            if addr:
                # hadoop fs -put localfile ... hdfs://[host|ip]/hadoopfile
                lcwd, rcwd = app.path().dcwd()
                for i in range(1, len(cmd) - 1):
                    cmd[i] = os.path.join(lcwd, cmd[i])

                CmdHelper.logcall(cmd)
            else:
                # hadoop fs -put localfile ... /hadoopfile
                lcwd, rcwd = app.path().dcwd()
                for i in range(1, len(cmd) - 1):
                    cmd[i] = os.path.join(lcwd, cmd[i])
                cmd[-1] = os.path.join(rcwd, cmd[-1])

                CmdHelper.logcall(cmd)

    @staticmethod
    def rm(cmd):
        # hadoop fs -rm [-f] [-r |-R] [-skipTrash] [-safely] URI [URI ...]

        if len(cmd) < 2 or cmd[-1].startswith('-'):
            return

        for i in range(1, len(cmd)):
            if not cmd[i].startswith('hdfs://') and not cmd[i].startswith('-'):
                cmd[i] = app.path().join(cmd[i])
        CmdHelper.logcall(cmd)

    @staticmethod
    def tail(cmd):

        # hadoop fs -tail [-f] URI
        if len(cmd) < 2 or cmd[-1].startswith('-'):
            return

        if not cmd[-1].startswith('hdfs://'):
            cmd[-1] = app.path().join(cmd[-1])
        CmdHelper.logcall(cmd)

    @staticmethod
    def touch(cmd):
        # hadoop fs -touch [-a] [-m] [-t TIMESTAMP] [-c] URI [URI ...]
        if len(cmd) < 2 or cmd[-1].startswith('-'):
            return

        for i in range(1, len(cmd)):
            if not cmd[i].startswith('hdfs://'):
                cmd[i] = app.path().join(cmd[i])
        CmdHelper.logcall(cmd)

    @staticmethod
    def truncate(cmd):
        # hadoop fs -truncate [-w] <length> <paths>
        if len(cmd) < 2 or cmd[-1].startswith('-'):
            return

        for i in range(1, len(cmd)):
            if not cmd[i].startswith('hdfs://'):
                cmd[i] = app.path().join(cmd[i])
        CmdHelper.logcall(cmd)

    @staticmethod
    def help(cmd):
        # hadoop fs -help
        if len(cmd) == 1:
            CmdHelper.logcall(cmd)
        elif cmd[1] == 'appendToFile':
            print('appendToFile <localsrc> ... <dst>')
        elif cmd[1] == 'cat':
            print('cat [-ignoreCrc] <src> ...')
        elif cmd[1] == 'copyFromLocal':
            print('copyFromLocal [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>')
        elif cmd[1] == 'copyToLocal':
            print('copyToLocal [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>')
        elif cmd[1] == 'get':
            print('get [-f] [-p] [-ignoreCrc] [-crc] <src> ... <localdst>')
        elif cmd[1] == 'head':
            print('head <file>')
        elif cmd[1] == 'ls':
            print('ls [-C] [-d] [-h] [-q] [-R] [-t] [-S] [-r] [-u] [-e] [<path> ...]')
        elif cmd[1] == 'mkdir':
            print('mkdir [-p] <path> ...')
        elif cmd[1] == 'moveFromLocal':
            print('moveFromLocal <localsrc> ... <dst>')
        elif cmd[1] == 'moveToLocal':
            print('moveToLocal <src> <localdst>')
        elif cmd[1] == 'mv':
            print('mv <src> ... <dst>')
        elif cmd[1] == 'put':
            print('put [-f] [-p] [-l] [-d] <localsrc> ... <dst>')
        elif cmd[1] == 'rm':
            print('rm [-f] [-r|-R] [-skipTrash] [-safely] <src> ...')
        elif cmd[1] == 'tail':
            print('tail [-f] [-s <sleep interval>] <file>')
        elif cmd[1] == 'touch':
            print('touch [-a] [-m] [-t TIMESTAMP ] [-c] <path> ...')
        elif cmd[1] == 'truncate':
            print('truncate [-w] <length> <path> ...')


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
    cp = 7

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
    except (EOFError, KeyboardInterrupt) as e:
        print(e)
