#!/bin/bash                                                                    
#https://spark.apache.org/docs/latest/submitting-applications.html
$SPARK_HOME/bin/spark-submit --class StreamingExampleWithHadoop --master spark://kenai:30135 --deploy-mode cluster  Assgn1/target/scala-2.11/tp_2.11-1.0.jar 11711 11712 11713

