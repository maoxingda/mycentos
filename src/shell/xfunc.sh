function xpath()
{
    echo -e "${PATH//:/\n}"
}

function readchar()
{
    read -r -n 1 -p 'yes or no' char; echo "$char"
}

function man()
{
    # shellcheck disable=SC2046
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
    if [[ $# -lt 2 ]]; then
        echo "usage:"
        echo "    putenv name value [value]..."
        return 1
    fi

    name=$1

    tmpfile=/tmp/$(date +%Y%m%d%H%M%S)

    echo "$name" > "$tmpfile"

    if grep -qE "^[0-9]" "$tmpfile"; then
        echo "the environment variable name can not start with digit"
        return 2
    fi

    envfile=/etc/profile.d/xenv.sh

    if grep -qE "$name=/" "$envfile"; then
        echo "[$name=$(printenv "$name")]"
        echo "are you sure want to reset it (y/n) "

        if [[ "y" != $(readchar) ]]; then
            return 3
        fi
    fi

    sudo sed -i.bak."$(date +%Y%m%d%H%M%S)" "/$name/d" "$envfile"

    echo -e "\n\n# $name" | sudo tee -a "$envfile" > /dev/null
    echo "export $name=$2" | sudo tee -a "$envfile" > /dev/null
    echo "export PATH=\$PATH:\$$name/bin" | sudo tee -a "$envfile" > /dev/null

    shift 2

    for arg in "$@"; do
        echo "export PATH=\$PATH:\$$name/$arg" | sudo tee -a "$envfile" > /dev/null
    done

    sudo sed -i.bak."$(date +%Y%m%d%H%M%S)" -e '/^$/{N;/\n$/D};' "$envfile" >& /dev/null

    exit
}
