alias egrep='grep -i -E'
alias qfind='sudo updatedb; locate'

export PS1="\[\e[37;40m\][\[\e[34;40m\]\u\[\e[37;40m\]@\[\e[35;40m\]\h \[\e[36;40m\]\W\[\e[0m\]]\\$ "

# colorful man page
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

# KAFKA_HOME
export KAFKA_HOME=/opt/module/kafka_2.11-0.11.0.0
export PATH=$PATH:$KAFKA_HOME/bin
