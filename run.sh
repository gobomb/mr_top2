#!/bin/sh
export JAVA_HOME=/usr/java/default
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
export HADOOP_PREFIX=/usr/local/hadoop
export CODE_SPACE=`pwd`

pushd ${HADOOP_PREFIX}
bin/hadoop com.sun.tools.javac.Main ${CODE_SPACE}/src/Top.java ${CODE_SPACE}/src/kaola/TopCounter.java ${CODE_SPACE}/src/kaola/TopSort.java ${CODE_SPACE}/src/kaola/Config.java
popd

pushd ${CODE_SPACE}/src
rm -f top.jar
jar cf top.jar Top*.class kaola/TopCounter*.class kaola/TopSort*.class kaola/Config*.class
popd

pushd ${HADOOP_PREFIX}
bin/hadoop jar ${CODE_SPACE}/src/top.jar Top /kaola/order/input /kaola/order/output
bin/hadoop fs -cat /kaola/order/intermediate/part-r-00000
bin/hadoop fs -cat /kaola/order/output/part-r-00000

bin/hadoop fs -get /kaola/order/output/part-r-00000 /root/output/output2
popd
