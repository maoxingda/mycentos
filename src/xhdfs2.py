#! /usr/local/bin/python3
import os
import re
import sys
import atexit
import readline
import argparse
from subprocess import call, check_output


# TODO add alias support
# TODO wildcard support
# TODO multi version support
# TODO cron write history cmd file support
class Watcher:

    def path_change(self, path):
        pass


class PathWatcher(Watcher):

    def __init__(self, cmd):
        self.__cmd = cmd
        self.__hdfs_choices_cached = {}
        readline.parse_and_bind("tab: complete")
        readline.set_completer(self.completer)
        readline.set_completer_delims(' /')

        self.__histfile = os.path.expandvars('${HOME}/.xhdfs_history')

        if os.path.exists(self.__histfile):
            readline.read_history_file(self.__histfile)

        atexit.register(self.__save_histfile)

        self.__local_choices = []
        self.__remote_choices = []

        self.__mk_remote_choices('/')
        self.__mk_local_choices(os.getcwd())

    def path_change(self, path):
        if app.is_hdfs_mode:
            self.__mk_remote_choices(path)
        else:
            self.__mk_local_choices(path)

    def choice_layout(self):
        if app.print_autocomp_words:
            if len(self.__local_choices) > 0:
                print('\n--->local')
            cnt = 0
            for choice in self.__local_choices:
                cnt += 1
                if cnt % 4 == 0:
                    print()
                print(choice.rjust(35), end=' ')

            if len(self.__remote_choices) > 0:
                print('\n\n--->hdfs')
            cnt = 0
            for choice in self.__remote_choices:
                cnt += 1
                if cnt % 4 == 0:
                    print()
                print(choice.rjust(35), end=' ')

            if len(self.__local_choices) > 0 or len(self.__remote_choices) > 0:
                print()

    def completer(self, text, state):
        try:
            words = []

            line = readline.get_line_buffer()
            line = [ele for ele in re.split(r'\s+', line) if ele != '']

            if len(line) == 1:
                words = self.__cmd.hdfscmd()
            elif len(line) > 1:
                line = line[-1]
                if line.find('/') == -1:
                    words = app.watcher().__choices()
                else:
                    if not line[-1].endswith('/'):
                        line = os.path.dirname(line)

                    local_path = os.path.join(os.getcwd(), line)

                    if os.path.exists(local_path):
                        lchoices = check_output(['ls', local_path]).decode('utf-8').split('\n')[:-1]
                        words.extend(lchoices)
                        # print(f'\n@lchoices: {lchoices}@')
                    else:
                        hdfs_path = os.path.join(app.path().cwd(), line)

                        if hdfs_path in self.__hdfs_choices_cached.keys():
                            words.extend(self.__hdfs_choices_cached[hdfs_path])
                        else:
                            rchoices = check_output(['hdfs', 'dfs', '-ls', hdfs_path]).decode('utf-8').split('\n')[1:-1]
                            rchoices = [rch.split('/')[-1] for rch in rchoices]
                            words.extend(rchoices)
                            self.__hdfs_choices_cached[hdfs_path] = rchoices
                        # print(f'\n@rchoices: {words}@')

            words = [word for word in words if word.startswith(text)]
            # print(f'\n@auto complete word: {words[state]}@')
            return words[state]
        except IndexError:
            # print('\n@out of range@')
            return None

    def __save_histfile(self):
        readline.write_history_file(self.__histfile)

    def __mk_local_choices(self, path):
        self.__local_choices = check_output(['ls', path]).decode('utf-8').split('\n')[:-1]

    def __mk_remote_choices(self, path):
        self.__remote_choices = check_output(['hdfs', 'dfs', '-ls', path]).decode('utf-8').split('\n')[1:-1]
        self.__remote_choices = [rch.split('/')[-1] for rch in self.__remote_choices]

    def __choices(self):
        if args.dyn_choices:
            lcwd, rcwd = app.path().dcwd()
            self.__mk_local_choices(lcwd)
            self.__mk_remote_choices(rcwd)
        return self.__local_choices + self.__remote_choices


class BasePath:

    def __init__(self, cwd, oldcwd):
        self.__cwd = None
        self.__oldcwd = None
        self.__watchers = []
        self.cwd = cwd
        self.oldcwd = oldcwd

    def add_watcher(self, watcher):
        self.__watchers.append(watcher)

    def dscribe(self, path):
        [watcher.path_change(path) for watcher in self.__watchers]

    @property
    def cwd(self):
        return self.__cwd

    @cwd.setter
    def cwd(self, val):
        if self.__cwd != val:
            self.dscribe(val)
        self.__cwd = val

    @property
    def oldcwd(self):
        return self.__oldcwd

    @oldcwd.setter
    def oldcwd(self, val):
        self.__oldcwd = val


class Path:

    def __init__(self):
        self.__local_path = Path.LocalPath(os.getcwd(), os.getcwd())
        self.__remote_path = Path.RemotePath('/', '/')
        self.__path = self.__remote_path

    class LocalPath(BasePath):

        def __init__(self, cwd, oldcwd):
            super().__init__(cwd, oldcwd)

        def cd(self, path):
            if 'cd' == path:
                self.oldcwd = self.cwd
                os.chdir(os.path.expandvars('${HOME}'))
                self.cwd = os.getcwd()
            elif path == '..':
                self.oldcwd = self.cwd
                os.chdir(os.pardir)
                self.cwd = os.getcwd()
            elif path == '-':
                os.chdir(self.oldcwd)
                self.oldcwd, self.cwd = self.cwd, self.oldcwd
            else:
                dst = Path.normalize(path, self.cwd)

                if os.path.exists(dst):
                    os.chdir(dst)
                    self.oldcwd = self.cwd
                    self.cwd = dst
                else:
                    print(f'cd: no such directory: {path}')

    class RemotePath(BasePath):

        def __init__(self, cwd, oldcwd):
            super().__init__(cwd, oldcwd)

        def cd(self, path):
            if 'cd' == path:
                self.oldcwd = self.cwd
                self.cwd = '/'
            elif path == '..':
                self.oldcwd = self.cwd
                self.cwd = os.path.dirname(self.cwd)
            elif path == '-':
                self.oldcwd, self.cwd = self.cwd, self.oldcwd
            else:
                dst = Path.normalize(path, self.cwd)

                if 0 == call(['hadoop', 'fs', '-test', '-d', dst]):
                    self.oldcwd = self.cwd
                    self.cwd = dst
                else:
                    print(f'cd: no such directory: {path}')

    def mode(self, mode):
        if mode:
            self.__path = self.__remote_path
        else:
            self.__path = self.__local_path

    def add_watcher(self, awatcher):
        self.__local_path.add_watcher(awatcher)
        self.__remote_path.add_watcher(awatcher)

    def prompt(self):
        local_bname = Path.__basename(self.__local_path.cwd)
        remote_bname = Path.__basename(self.__remote_path.cwd)

        prompt = '>'
        cmd_mode_prompt = 'hdfs'

        if not app.is_hdfs_mode:
            prompt += '>>'
            cmd_mode_prompt = 'local'

        return f'\n[{cmd_mode_prompt}][{local_bname}:{remote_bname}]{prompt} '

    def cwd(self):
        return self.__path.cwd

    def dcwd(self):
        return self.__local_path.cwd, self.__remote_path.cwd

    def cd(self, path):
        self.__path.cd(path)

    @staticmethod
    def normalize(path, cwd):
        if not path.startswith('hdfs://'):
            path = os.path.join(cwd, path)
            path = os.path.normpath(path)
        return path

    @staticmethod
    def __basename(path: str):
        if len(path) > 0:
            if path != '/':
                if path.endswith('/'):
                    path = os.path.basename(os.path.dirname(path))
                else:
                    path = os.path.basename(path)
        else:
            path = '/'

        return path


class Command:

    def __init__(self):
        self.__CMD_PWD = 'pwd'
        self.__CMD_EXIT = 'exit'
        self.__CMD_LOG_PRINT = 'log-print'
        self.__CMD_PRINT = 'cmd-print'
        self.__CMD_HISTORY = 'history'
        self.__CMD_CD = 'cd'
        self.__CMD_HDFS_ADDR = 'hdfs-addr'
        self.__CMD_ALIAS = 'alias'

        self.__CMD_CAT = 'cat'
        self.__CMD_CP = 'cp'
        self.__CMD_GET = 'get'
        self.__CMD_LS = 'ls'
        self.__CMD_LL = 'll'
        self.__CMD_LSR = 'lsr'
        self.__CMD_MKDIR = 'mkdir'
        self.__CMD_MV = 'mv'
        self.__CMD_PUT = 'put'
        self.__CMD_RM = 'rm'
        self.__CMD_RMR = 'rmr'
        self.__CMD_TAIL = 'tail'
        self.__CMD_TOUCH = 'touch'
        self.__CMD_USAGE = 'usage'

        self.__common_alias: dict[str, list[str]] = {
            self.__CMD_LSR: [self.__CMD_LS, '-R'],
            self.__CMD_RMR: [self.__CMD_RM, '-R']
        }
        self.__hdfs_alias: dict[str, list[str]] = {
            self.__CMD_LL: [self.__CMD_LS, '']
        }
        self.__local_alias: dict[str, list[str]] = {
            self.__CMD_LL: [self.__CMD_LS, '-l']
        }

        self.__cmd: list[str] = []
        self.__hdfscmd: list[str] = ['!']
        for attr in dir(self):
            if attr.__contains__('__CMD'):
                self.__hdfscmd.append(self.__getattribute__(attr))

    def hdfscmd(self):
        return self.__hdfscmd

    def parse(self, cmd):
        self.__cmd = re.split(r'\s+', cmd)
        self.__cmd = [ele for ele in self.__cmd if ele != '']

        if 0 == len(self.__cmd):
            return

        self.__handle_alias()

        cname = self.__cmd[0]

        if cname == self.__CMD_EXIT:
            sys.exit()
        elif cname == '!':
            app.is_hdfs_mode = not app.is_hdfs_mode
        elif cname == self.__CMD_PWD:
            print(os.path.normpath(app.path().cwd()))
        elif cname == self.__CMD_ALIAS:
            self.__alias()
        elif cname == self.__CMD_LOG_PRINT:
            app.print_autocomp_words = not app.print_autocomp_words
        elif cname == self.__CMD_PRINT:
            app.print_cmd = not app.print_cmd
        elif cname == self.__CMD_HISTORY:
            Command.__history()
        elif cname == self.__CMD_CD:
            app.path().cd(self.__cmd[-1])
        elif cname == self.__CMD_HDFS_ADDR:
            Command.__hdfs_addr()

        # system commands
        elif not app.is_hdfs_mode:
            self.__exec()

        # hdfs commands
        elif cname == self.__CMD_CAT:
            self.__cat()
        elif cname == self.__CMD_CP:
            self.__cp()
        elif cname == self.__CMD_GET:
            self.__get()
        elif cname == self.__CMD_LS:
            self.__ls()
        elif cname == self.__CMD_MKDIR:
            self.__mkdir()
        elif cname == self.__CMD_MV:
            self.__mv()
        elif cname == self.__CMD_PUT:
            self.__put()
        elif cname == self.__CMD_RM:
            self.__rm()
        elif cname == self.__CMD_TAIL:
            self.__tail()
        elif cname == self.__CMD_TOUCH:
            self.__touch()
        elif cname == self.__CMD_USAGE:
            self.__usage()
        else:
            return self.__not_find_cmd()

    # commands implementation
    def __cat(self):
        # cat URI
        if self.__non_options() < 2:
            return

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd)):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], rcwd)
        
        self.__exec()

    def __cp(self):
        # cp [-f] URI <dest>
        non_options = self.__non_options()
        if non_options < 2:
            return
        elif non_options == 2:
            self.__cmd.append(app.path().cwd())

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd)):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], rcwd)

        self.__exec()

    def __get(self):
        # get [-f] URI
        if self.__non_options() < 2:
            return

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd) - 1):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], rcwd)
        self.__cmd[-1] = Path.normalize(self.__cmd[-1], lcwd)

        self.__exec()

    def __ls(self):
        # ls [-d] [-h] [-R] <args>
        nopt = self.__non_options()

        if 1 == nopt:
            self.__cmd.append(app.path().dcwd()[1])

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd)):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], rcwd)

        self.__exec()

    def __mkdir(self):
        # mkdir [-p] <paths>
        if self.__non_options() < 2:
            return

        if '-p' not in self.__cmd:
            self.__cmd.insert(1, '-p')

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd)):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], rcwd)

        self.__exec()

    def __mv(self):
        # mv URI <dest>
        non_options = self.__non_options()
        if non_options < 2:
            return
        elif non_options == 2:
            self.__cmd.append(app.path().cwd())

        for i in range(1, len(self.__cmd)):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], app.path().cwd())

        self.__exec()

    def __put(self):
        # put [-f] [-p] [-l] [-d] <localsrc> <dst>
        non_options = self.__non_options()
        if non_options < 2:
            return
        elif non_options == 2:
            self.__cmd.append(app.path().cwd())

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd) - 1):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], lcwd)
        self.__cmd[-1] = Path.normalize(self.__cmd[-1], rcwd)

        self.__exec()

    def __rm(self):
        # rm [-R] URI
        if self.__non_options() < 2:
            return

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd)):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], rcwd)

        self.__exec()

    def __tail(self):
        # tail [-f] URI
        if self.__non_options() < 2:
            return

        self.__cmd[-1] = Path.normalize(self.__cmd[-1], app.path().cwd())

        self.__exec()

    def __touch(self):
        # touchz URI
        if self.__non_options() < 2:
            return

        self.__cmd[0] += 'z'

        lcwd, rcwd = app.path().dcwd()

        for i in range(1, len(self.__cmd)):
            if not self.__cmd[i].startswith('-'):
                self.__cmd[i] = Path.normalize(self.__cmd[i], rcwd)

        self.__exec()

    @staticmethod
    def __history():
        hist = os.path.expandvars('${HOME}/.xhdfs_history')
        if os.path.exists(hist):
            with open(hist) as f:
                for cmd in f.readlines():
                    print(cmd, end='')

    def __usage(self):
        # usage command
        if self.__non_options() < 2:
            return

        self.__exec()

    def __exec(self):
        cmd = ' '.join(self.__cmd)

        if app.is_hdfs_mode:
            cmd = 'hdfs dfs -' + cmd

        if app.print_cmd:
            print(cmd)

        call(cmd, shell=True)

    def __non_options(self):
        return len([ele for ele in self.__cmd if not ele.startswith('-')])

    def __not_find_cmd(self):
        cmd = ''.join(self.__cmd)
        cmd = f'command not find: {cmd}'
        print(f'\033[1;32;47m{cmd}\033[0m')

    def __handle_alias(self):
        key = self.__cmd[0]
        if key in self.__common_alias.keys():
            self.__cmd[0] = self.__common_alias[key][0]
            options = self.__common_alias[key][1]
            if len(options) > 0:
                self.__cmd.insert(1, options)
            return

        if app.is_hdfs_mode and key in self.__hdfs_alias.keys():
            self.__cmd[0] = self.__hdfs_alias[key][0]
            options = self.__hdfs_alias[key][1]
            if len(options) > 0:
                self.__cmd.insert(1, options)
        elif key in self.__local_alias.keys():
            self.__cmd[0] = self.__local_alias[key][0]
            options = self.__local_alias[key][1]
            if len(options) > 0:
                self.__cmd.insert(1, options)

    @staticmethod
    def __hdfs_addr():
        if os.getenv('HADOOP_HOME'):
            core_site_file = os.path.join(os.environ['HADOOP_HOME'], 'etc/hadoop/core-site.xml')
            cmd = f'grep -B 2 -A 1 \'hdfs://\' {core_site_file}'
            if app.print_cmd:
                print(cmd)
            call(cmd, shell=True)
        else:
            print('HADOOP_HOME environment variable not set!')

    def __alias(self):
        # alias alia=cmd [-h|-l|-c] [-option1, ...]
        if len(self.__cmd) == 1:
            self.__print_alias()
            return

        what = re.fullmatch(r'(?P<alia>\w+)\s*='
                            r'\s*(?P<cmd>\w+)(\s+-(?P<mode>[hlc]))?(?P<options>\s+-\w+)*', ' '.join(self.__cmd[1:]))
        if what:
            alia = what.group('alia')
            cmd = what.group('cmd')
            mode = what.group('mode')
            options = what.group('options').lstrip(' ')
            val = [cmd]
            if options:
                options = options.split(r'\s+')
                val += options
            if not mode or mode == 'h':
                self.__hdfs_alias[alia] = val
            elif mode == 'l':
                self.__local_alias[alia] = val
            elif mode == 'c':
                self.__common_alias[alia] = val

    def __print_alias(self):
        alias = self.__common_alias

        if app.is_hdfs_mode:
            alias.update(self.__hdfs_alias)
        else:
            alias.update(self.__local_alias)

        for k, v in alias.items():
            v = [i for i in v if len(i) > 0]
            print(f'alias {k}=' + ' '.join(v))


class Main:

    def main(self):

        while True:
            self.watcher().choice_layout()

            cmd = input(self.path().prompt())

            self.__cmd.parse(cmd)

    def __init__(self):
        self.__hdfs = True
        self.print_cmd = args.cmd_print
        self.print_autocomp_words = args.log_print

        self.__cmd = Command()
        self.__path = Path()
        self.__addr = args.hdfs_address

        self.init()
        self.__watcher = PathWatcher(self.__cmd)
        self.__path.add_watcher(self.__watcher)

    def init(self):
        if self.__addr:
            if os.getenv('HADOOP_HOME'):
                core_site_file = os.path.join(os.environ['HADOOP_HOME'], 'etc/hadoop/core-site.xml')
                with open(core_site_file) as f:
                    core_site_conf = f.readlines()
                for row, line in enumerate(core_site_conf):
                    if line.find('hdfs://') != -1:
                        core_site_conf[row] = ' ' * 8 + f'<value>hdfs://{self.__addr}</value>\n'
                        break
                with open(core_site_file, 'w') as f:
                    f.writelines(core_site_conf)
            else:
                print('HADOOP_HOME environment variable not set!')

    @property
    def is_hdfs_mode(self):
        return self.__hdfs

    @is_hdfs_mode.setter
    def is_hdfs_mode(self, val):
        self.__hdfs = val
        self.path().mode(val)

    def path(self):
        return self.__path

    def watcher(self):
        return self.__watcher


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c', '--cmd-print', action='store_true', help='if print exec command')
    parser.add_argument('-l', '--log-print', action='store_true', help='if print auto completion candidate words')
    parser.add_argument('-s', '--hdfs-address', help='hdfs cluster address')
    parser.add_argument('-d', '--dyn-choices', action='store_true', help='if dynamic make choices')
    return parser.parse_args()


if __name__ == '__main__':
    try:
        args = parse_args()
        app = Main()
        app.main()
    except (EOFError, KeyboardInterrupt):
        pass
