#!/bin/bash
javac -classpath ${HADOOP_HOME}/hadoop-core*.jar -d genetic_classes Genetic.java
jar -cvf genetic.jar -C genetic_classes/ .
rm -rf output/
bin/hadoop jar genetic.jar org.myorg.Genetic gen_input2 output 
