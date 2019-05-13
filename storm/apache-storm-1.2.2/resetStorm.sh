./stopWorkers.sh
ssh atlanta "unlink /tmp/sarmst-storm/supervisor.sock"
ssh columbia "unlink /tmp/sarmst-storm/supervisor.sock; cd /s/chopin/k/grad/sarmst/cs535/zookeeper-3.4.13; ./stop.sh; ./start.sh; supervisord -c /s/chopin/k/grad/sarmst/cs535/storm/apache-storm-1.2.2/zk-supervisord.conf;"
ssh atlanta "supervisord -c /s/chopin/k/grad/sarmst/cs535/storm/apache-storm-1.2.2/nimbus-supervisord.conf"
./startWorkers.sh
