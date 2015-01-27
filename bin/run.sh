
CPFILES='.:./cloudos.jar'
for f in $(find lib/*.jar); do
	#echo $f
	CPFILES=$CPFILES':'$f
done
echo 'HERE'
echo $CPFILES

#java -cp $CPFILES ${*:2} '--properties='$1'cloudos.properties' > $1'log.txt' &
#disown
echo java -cp $CPFILES $*
java -server -XX:+UseParallelGC -XX:+UseParallelOldGC -cp $CPFILES $* 1> 'log.txt' 2> 'err.txt' &
disown
