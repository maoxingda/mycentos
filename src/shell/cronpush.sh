#! /bin/bash
# .-------- minute (0 - 59)
# | .------ hour (0 - 23)
# | | .---- day of month (1 - 31)
# | | | .-- month (1 - 12) OR jan,feb,mar,apr ...
# | | | | . day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# | | | | |
# * * * * * user-name command to be executed

if [ ! -d "/home/maoxd/github" ]; then
    mkdir "/home/maoxd/github"
    git clone git@github.com:maoxingda/mycentos.git
fi

cd /home/maoxd/github/mycentos/src/shell || exit

git rebase
git pull

files=(custom-env.sh custom-functions.sh)

for file in "${files[@]}"; do
#  sudo rm -v -f "$file"
  sudo cp -v /etc/profile.d/"$file" "$file"

  git add "$file"

  git commit -m 'cron push...'
done

git push
