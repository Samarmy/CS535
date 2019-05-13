cd $HADOOP_CONF_DIR
for i in `cat workers`
do
ssh $i "rm -rf /tmp/hadoop-$USER"
done

