for i in `cat workers`
do
ssh $i "unlink /tmp/sarmst-storm/supervisor.sock"
done
