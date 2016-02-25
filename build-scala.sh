#!/usr/bin/env bash

rm -rf ./build/org/apache/spark/deploy
scalac -classpath $ASSEMBLY_JAR -d ./build/ ./src/main/scala/NodeSparkSubmit.scala