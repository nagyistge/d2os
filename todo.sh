cd $CLOUDOS_HOME

./clear.sh
grep -in '//TODO' -r ./src/* | awk -F: '{pos = match($3, "//TODO"); str = substr($3,pos+6); gsub(/^[ \t]+/, "", str); gsub(/[ \t]+$/, "", str); print $1":"$2"\n   "str}'
