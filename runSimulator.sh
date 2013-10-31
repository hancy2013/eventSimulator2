#!/bin/bash
echo start with $1
CP=build/sim2.jar
for jar in lib/*.jar extlib/*.jar
do
	CP=$CP:${jar}
done
if [ "x$1x" == "xx" ]
  then PAR=S 
  else PAR=$1
fi
echo start with parpam:  $PAR
java -Xmx800m -classpath $CP home.vitaly.simulator.RunSimulator $PAR

