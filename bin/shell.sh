cd $CLOUDOS_HOME/bin/

CPFILES='./cloudos.jar'
for f in $(find ../lib/*.jar); do
	#echo $f
	CPFILES=$CPFILES':'$f
done

java -cp $CPFILES cloudos.cli.Shell
