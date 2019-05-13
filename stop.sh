ssh kenai "$SPARK_HOME/sbin/stop-all.sh"
ssh albany "$HADOOP_HOME/sbin/yarn-stop.sh"
ssh austin "$HADOOP_HOME/sbin/stop-dfs.sh"
ssh austin "$HADOOP_HOME/sbin/stop-all.sh"
rm -r /s/chopin/k/grad/sarmst/cs535/hadoopConf/*app*
rm -r /s/chopin/k/grad/sarmst/cs535/hadoopConf/*driver*
rm -r /s/chopin/k/grad/sarmst/cs535/hadoopConf/logs/*

