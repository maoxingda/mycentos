color=41

function funcargs() {
    for arg in "$@"; do
        echo "$arg"
    done
}

function xecho()
{
    for arg in "$@"; do
        echo -ne "\e[${color}${arg} \e[0m"
    done

    echo
}

function xecholn()
{
    if [[ $# != 1 ]] ; then
        echo -e "\e[${color}musage:\e[0m"
        echo -e "\e[${color}m    xecholn message\e[0m"
    fi

    echo -e "\e[${color}m$1\e[0m"
}

function yesorno()
{
    read -p "" answer

    if [[ $answer == no || $answer == n || "$answer" == "" ]] ; then
        return 1
    fi

    if [[ $answer == yes || $answer == y ]] ; then
        return 0
    fi

    return 2
}

function xprintenv()
{
    if [[ $# != 1 ]] ; then
        echo -e "\e[${color}musage:\e[0m"
        echo -e "\e[${color}m    xprintenv name\e[0m"
        echo && return $?
    fi

    name=$(python -c 'import sys; \
        print(sys.argv[1].upper())' $1)

    if [[ $name == PATH ]] ; then
        printenv PATH | sed "s/:/\n/g" | sed "s/\/\//\//g"
        return $?
    fi

    echo -e "\e[${color}m$(printenv $name)\e[0m"
}

function man()
{
    env \
    LESS_TERMCAP_mb=$(printf "\e[1;31m") \
    LESS_TERMCAP_md=$(printf "\e[1;31m") \
    LESS_TERMCAP_me=$(printf "\e[0m") \
    LESS_TERMCAP_se=$(printf "\e[0m") \
    LESS_TERMCAP_so=$(printf "\e[1;44;33m") \
    LESS_TERMCAP_ue=$(printf "\e[0m") \
    LESS_TERMCAP_us=$(printf "\e[1;32m") \
    man "$@"
}

function putenv()
{
    if [[ $# < 2 ]] ; then
        xecholn "usage:"
        xecholn "    putenv name value [value]..."
        echo && return $?
    fi

    name=$(python -c 'import sys; \
        print(sys.argv[1].upper())' $1)

    echo $name | grep -E "^[0-9]" >& /dev/null

    if [[ $? == 0 ]] ; then
        xecholn "the environment variable name can not start with digit"
        echo && return $?
    fi

    grep -E "$name=/" /etc/profile.d/maoxd-env.sh >& /dev/null
    alset=$?

    if [[ $alset == 0 ]] ; then
        xecholn "[$name=$(printenv $name)]"
        xecho "are you sure want to reset it, yes or no? " && yesorno
    fi

    if [[ $alset == 0 && $? != 0 ]] ; then
        return $?
    fi

    sudo sed -i.bak.$(date +%Y%m%d%H%M%S) "/$name/d" /etc/profile.d/maoxd-env.sh

    echo >> /etc/profile.d/maoxd-env.sh
    echo >> /etc/profile.d/maoxd-env.sh

    case $# in
    2)
        echo "# $name" >> /etc/profile.d/maoxd-env.sh
        echo "export $name=$2" >> /etc/profile.d/maoxd-env.sh
        echo "export PATH=\$PATH:\$$name/bin" >> /etc/profile.d/maoxd-env.sh
        ;;

    3)
        echo "# $name" >> /etc/profile.d/maoxd-env.sh
        echo "export $name=$2" >> /etc/profile.d/maoxd-env.sh
        echo "export PATH=\$PATH:\$$name/bin" >> /etc/profile.d/maoxd-env.sh
        echo "export PATH=\$PATH:\$$name/$3" >> /etc/profile.d/maoxd-env.sh
        ;;
    *)
        return $?
        ;;
    esac

    source /etc/profile

    sudo sed -i.bak.$(date +%Y%m%d%H%M%S) -e '/^$/{N;/\n$/D};' /etc/profile.d/maoxd-env.sh >& /dev/null

    return $?
}
