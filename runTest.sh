declare -i n=1
if [ $# = 1 ]
then
  n=$1
fi

echo "Running the tester $n times..."
it=1
while [ $it -le $n ]
do
  echo "==================== Iteration $it ===================="
  ./simpledynamo-grading.osx  app/build/outputs/apk/debug/app-debug.apk 2>&1 | tee testOutput.txt

  score=$(grep -o "Total score: [[:digit:]]*" testOutput.txt)
  
  if [ "$score" != "Total score: 10" ]
  then
    echo "Total score is not 10! Please debug. :("
    break
  fi
  
  (( it++ ))
done
