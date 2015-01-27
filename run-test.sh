cd $CLOUDOS_HOME

CPFILES='./tests/build/'
for f in ./lib/*.jar; do
	CPFILES=$CPFILES':'$f
done

mkdir ./classes
mkdir ./tests/build/
cp -r ./src/* ./tests/build/
cp -r ./tests/src/* ./tests/build/
cp -r ./tests/mock/* ./tests/build/

JFILES=''
for f in $(find ./tests/build/ -name *.java); do
	JFILES=$JFILES' '$f
done

rm -r ./classes/*
rm cloudos-tests.jar

javac -server -d ./classes/ -cp $CPFILES $JFILES $*

rm -r ./tests/build/

if [[ $? -eq 0 ]]; then

jar cf cloudos-tests.jar -C classes/ .

java -cp $CPFILES':cloudos-tests.jar' org.junit.runner.JUnitCore cloudos.kernel.info.SSHInfoTest cloudos.kernel.info.NodeInfoTest cloudos.kernel.EnvironmentInfoTest cloudos.kernel.SystemCallRequestTest cloudos.kernel.SystemCallReplyTest cloudos.kernel.ModuleControllerTest

fi
rm -r ./classes
