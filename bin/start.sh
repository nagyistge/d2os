cd $CLOUDOS_HOME/bin/

rm cloudos.tar.gz
tar czf cloudos.tar.gz ../lib run.sh log4j.properties cloudos.properties environment.xml cloudos.jar rm-slave.sh

CPFILES='./cloudos.jar'
for f in $(find ../lib/*.jar); do
	#echo $f
	CPFILES=$CPFILES':'$f
done

java -cp $CPFILES cloudos.kernel.KernelCore -p ./cloudos.properties
