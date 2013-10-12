#!/bin/bash
#IntegerSort compilation script.

if [ ! -d "classes" ]; then
    echo "Create classes folder..."
    mkdir classes
fi

echo "Compiling..."
javac -sourcepath src -classpath $CLASSPATH/hadoop-*core*.jar src/com/yullage/bigdata2013/IntegerSort.java -d classes

echo "Creating JAR file..."
jar -cvf IntegerSort.jar -C classes/ .

echo "Done."
