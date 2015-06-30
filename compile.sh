rm -r target
mkdir target
find -name "*.java" > source.txt
javac -classpath lib/hadoop-core-1.0.3.jar -d target @source.txt
jar -cvf hw3.jar -C target/ .
