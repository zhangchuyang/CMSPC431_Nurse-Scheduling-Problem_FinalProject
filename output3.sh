echo "Output for question 3:"
echo " "
echo "last Name | first Name | date | day | role | start | end | phone"
sed 's/,/|/g' $1 | sort
