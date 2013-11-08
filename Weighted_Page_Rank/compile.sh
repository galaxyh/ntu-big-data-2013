#!/bin/bash
# Weighted page rank compilation script

if [ ! -d "classes" ]; then
    echo "Create classes folder..."
    mkdir classes
fi

echo "Compiling..."

javac -sourcepath src -classpath '/opt/hadoop-1.2.1/*:/opt/hadoop-1.2.1/lib/*:/opt/hama-0.6.3/*:/opt/hama-0.6.3/lib/*' src/com/yullage/bigdata2013/WeightedPageRank.java -d classes

echo "Creating JAR file..."
jar -cvf WeightedPageRank.jar -C classes/ .

echo "Done."
