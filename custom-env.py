#! /usr/bin/python3
# -*- coding: utf-8 -*-
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-f', '--file', default='/etc/profile.d/custom.sh'
                        , help='the file name will be created')

    args = parser.parse_args()

    lines = ['alias egrep=\'grep -i -E\'\n',
             'alias qfind=\'sudo updatedb; locate\'\n\n',
             r'export PS1="\[\e[37;40m\][\[\e[34;40m\]\u'
             r'\[\e[37;40m\]@\[\e[35;40m\]\h '
             r'\[\e[36;40m\]\W\[\e[0m\]]\\$ "' + '\n\n',
             '# colorful man page\n',
             'function man()\n',
             '{\n',
             '    env \\\n',
             '    LESS_TERMCAP_mb=$(printf "\e[1;31m") \\\n',
             '    LESS_TERMCAP_md=$(printf "\e[1;31m") \\\n',
             '    LESS_TERMCAP_me=$(printf "\e[0m") \\\n',
             '    LESS_TERMCAP_se=$(printf "\e[0m") \\\n',
             '    LESS_TERMCAP_so=$(printf "\e[1;44;33m") \\\n',
             '    LESS_TERMCAP_ue=$(printf "\e[0m") \\\n',
             '    LESS_TERMCAP_us=$(printf "\e[1;32m") \\\n',
             '    man "$@"\n',
             '}'
             ]

    with open(args.file, 'w') as file:
        file.writelines(lines)
