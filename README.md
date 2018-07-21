# EOS ZMQ plugin receiver tools


```
sudo apt-get install cpanminus libzmq5-dev mariadb-server \
libdbi-perl libdbd-mysql-perl libexcel-writer-xlsx-perl

sudo cpanm ZMQ::LibZMQ3

sudo mysql <sql/eos_zmq_dbcreate.sql 

perl scripts/eos_zmq_dumper.pl  --connect=tcp://127.0.0.1:5000 | tee ~/eos.log
```

