cd $CLOUDOS_HOME

CPFILES='./src/'
for f in ./lib/*.jar; do
	CPFILES=$CPFILES':'$f
done

if [[ $1 == "kernel" ]]; then
	JFILES=''
	for f in $(find ./src/cloudos/ -name *.java); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./doc
	mkdir ./doc/cloudos
	javadoc -d ./doc/cloudos/ -classpath $CPFILES $JFILES
fi

if [[ $1 == "dfs" ]]; then
	JFILES=''
	for f in $(find ./src/dfs/ -name *.java); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./doc
	mkdir ./doc/dfs
	javadoc -d ./doc/dfs/ -classpath './bin/cloudos.jar:'$CPFILES $JFILES
fi

if [[ $1 == "dpm" ]]; then
	JFILES=''
	for f in $(find ./src/dpm/ -name *.java); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./doc
	mkdir ./doc/dpm
	javadoc -d ./doc/dpm/ -classpath './bin/cloudos.jar:'$CPFILES $JFILES
fi

if [[ $1 == "watershed" ]]; then
	JFILES=''
	for f in $(find ./src/watershed/ -name *.java); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./doc
	mkdir ./doc/watershed
	javadoc -d ./doc/watershed/ -classpath './bin/cloudos.jar:./bin/dfs.jar:'$CPFILES $JFILES
fi

if [[ $1 == "mapred" ]]; then
	JFILES=''
	for f in $(find ./src/mapred/ -name *.java); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./doc
	mkdir ./doc/mapred
	javadoc -d ./doc/mapred/ -classpath './bin/cloudos.jar:./bin/dfs.jar:./bin/dpm.jar:'$CPFILES $JFILES
fi

