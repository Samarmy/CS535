#!/bin/bash                                                                    
#https://spark.apache.org/docs/latest/submitting-applications.html
$SPARK_HOME/bin/spark-submit --class Graphx --master spark://columbia:30135 --deploy-mode client Assgn1/target/scala-2.11/tp_2.11-1.0.jar

