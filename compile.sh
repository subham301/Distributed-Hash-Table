#!/bin/bash

SUCCESS=1
javac RingElement.java
if test $? -ne 0
then
  $SUCCESS=0
fi

javac ServerInfo.java
if test $? -ne 0
then
  $SUCCESS=0
fi

javac Client.java
if test $? -ne 0
then
  $SUCCESS=0
fi

javac AppServerTaskHandler.java
if test $? -ne 0
then
  $SUCCESS=0
fi

javac AppServer.java
if test $? -ne 0
then
  $SUCCESS=0
fi

javac LoadBalancerTaskHandler.java
if test $? -ne 0
then
  $SUCCESS=0
fi

javac LoadBalancer.java
if test $? -ne 0
then
  $SUCCESS=0
fi

if test $SUCCESS -eq 1
then
  echo "All the files compiled successfully!"
else 
  echo "There are some errrors during compilation!"
fi