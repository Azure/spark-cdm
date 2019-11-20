#!/bin/bash

sbt 'set test in assembly := {}' clean assembly

cp ./target/scala-2.11/*.jar /tmp/spark-cdm.jar


