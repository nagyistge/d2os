#cd $CLOUDOS_HOME

CPFILES='./src/'
for f in ./lib/*.jar; do
	CPFILES=$CPFILES':'$f
done

if [[ $1 == "kernel" ]]; then
	JFILES=''
	for f in $(find ./src/d2os/ -name "*.java"); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./classes
	rm ./bin/d2os.jar
	#echo $JFILES
	#javac -server -d ./classes/ -cp $CPFILES $JFILES -Xlint
	javac -server -d ./classes/ -cp $CPFILES $JFILES
	if [[ $? -eq 0 ]]; then
		jar cf ./bin/d2os.jar -C classes/ .
	fi
	rm -r ./classes
fi

if [[ $1 == "dfs" ]]; then
	JFILES=''
	for f in $(find ./src/dfs/ -name "*.java"); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./classes
	rm ./bin/dfs.jar
	#javac -server -d ./classes/ -cp $CPFILES $JFILES -Xlint
	javac -server -d ./classes/ -cp './bin/cloudos.jar:'$CPFILES $JFILES
	if [[ $? -eq 0 ]]; then
		jar cf ./bin/dfs.jar -C classes/ .
	fi
	rm -r ./classes
fi

if [[ $1 == "dpm" ]]; then
	JFILES=''
	for f in $(find ./src/dpm/ -name "*.java"); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./classes
	rm ./bin/dpm.jar
	#javac -server -d ./classes/ -cp $CPFILES $JFILES -Xlint
	javac -server -d ./classes/ -cp './bin/cloudos.jar:'$CPFILES $JFILES
	if [[ $? -eq 0 ]]; then
		jar cf ./bin/dpm.jar -C classes/ .
	fi
	rm -r ./classes
fi

if [[ $1 == "watershed" ]]; then
	JFILES=''
	for f in $(find ./src/watershed/ -name "*.java"); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./classes
	rm ./bin/watershed.jar
	#javac -server -d ./classes/ -cp $CPFILES $JFILES -Xlint
	javac -server -d ./classes/ -cp './bin/cloudos.jar:./bin/dfs.jar:'$CPFILES $JFILES
	if [[ $? -eq 0 ]]; then
		jar cf ./bin/watershed.jar -C classes/ .
	fi
	rm -r ./classes
fi

if [[ $1 == "mapred" ]]; then
	JFILES=''
	for f in $(find ./src/mapred/ -name "*.java"); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./classes
	rm ./bin/mapred.jar
	#javac -server -d ./classes/ -cp $CPFILES $JFILES -Xlint
	javac -server -d ./classes/ -cp './bin/cloudos.jar:./bin/dfs.jar:./bin/dpm.jar:'$CPFILES $JFILES
	if [[ $? -eq 0 ]]; then
		jar cf ./bin/mapred.jar -C classes/ .
	fi
	rm -r ./classes
fi

if [[ $1 == "samples" ]]; then
	JFILES=''
	for f in $(find ./src/sample/watershed/ -name "*.java"); do
		JFILES=$JFILES' '$f
	done
	
	mkdir ./classes
	rm ./bin/samples.jar
	#javac -server -d ./classes/ -cp $CPFILES $JFILES -Xlint
	javac -server -d ./classes/ -cp './bin/cloudos.jar:./bin/dfs.jar:./bin/watershed.jar:'$CPFILES $JFILES
	if [[ $? -eq 0 ]]; then
		jar cf ./bin/samples.jar -C classes/ .
	fi
	rm -r ./classes
fi
