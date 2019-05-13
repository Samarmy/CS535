for i in `cat workers`
do
ssh $i "mkdir /tmp/sarmst-storm; supervisord -c /s/chopin/k/grad/sarmst/cs535/storm/apache-storm-1.2.2/worker-supervisord.conf;"
done
