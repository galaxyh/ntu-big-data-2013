#!/bin/bash
#PageRank compilation script.

if [ ! -d "classes" ]; then
    echo "Create classes folder..."
    mkdir classes
fi

echo "Compiling..."
javac -sourcepath src -classpath $CLASSPATH/hadoop-*core*.jar src/com/yullage/bigdata2013/PageRank.java -d classes

echo "Creating JAR file..."
jar -cvf PageRank.jar -C classes/ .

echo "Done."
