echo "Output for question 1:"
echo " "
echo "start | end | date | day | department | role"
sed 's/,/|/g' $1
