## node_mgr

Each machine node runs such an server instance, which operates kunlun storage nodes(SN) and/or computing nodes(CN) that run on the computer server, in response to requests initiated by computing nodes and/or cluster_mgr module to start/stop/create/drop/backup/restore a CN/SN. 


## the http post sample int the test path

http_post_para.cc
http_post_file.cc
./http_post_para http://127.0.0.1:9998 "{\"key\":\"value\"}"
./http_post_file http://127.0.0.1:9998 "{\"key\":\"value\"}" ./test.json

it can use curl, but curl can not send para and file together.
curl -d "{\"job_type\":\"get_node\",\"node_type\":\"meta_node\"}" http://127.0.0.1:9998/


## http post paramemter cmd to cluster_mgr

1. get meta_node/storage_node/computer_node from cluster_mgr, with local ip :
{"job_type":"get_node","node_type":"meta_node","node_ip0":"127.0.0.1","node_ip1":"192.168.207.145"}

2. it can get all node without any node_type or node_ip :
{"job_type":"get_node"}


## http post paramemter cmd to node_mgr

1. get node of meta_node/storage_node/computer_node from node_mgr, the cmd is same as the cluster_mgr, but result is different.
{"job_type":"get_node","node_type":"meta_node"}

curl -d "{\"job_type\":\"get_node\",\"node_type\":\"meta_node\"}" http://127.0.0.1:9998/
./http_post_para http://127.0.0.1:9998 "{\"job_type\":\"get_node\",\"node_type\":\"meta_node\"}"

2. get info from node_mgr, it will wait a moment to get machine info
{"job_type":"get_info"}

curl -d "{\"job_type\":\"get_info\"}" http://127.0.0.1:9998/
./http_post_para http://127.0.0.1:9998 "{\"job_type\":\"get_info\"}"

3. update node info notify by cluster_mgr, and node_mgr will get all node from cluster_mgr.
{"job_type":"update_node"}

4. delete a table or binlong
{"job_id":"id123456","job_type":"delete","file_type":"table",
"s_ip":"127.0.0.1","s_port":6007,"s_user":"pgx","s_pwd":"pgx_pwd","s_dbname":"postgres_$$_public","s_table_name":"t13"}

{"job_id":"id123456","job_type":"delete","file_type":"binlog",
"s_ip":"127.0.0.1","s_port":6007, "s_user":"pgx","s_pwd":"pgx_pwd"}

5. send a file to another mysql instance
{"job_id":"id123456","job_type":"send","file_type":"talbe",
"s_ip":"127.0.0.1","s_port":6004,"s_user":"pgx","s_pwd":"pgx_pwd","s_dbname":"postgres_$$_public","s_table_name":"t12",
"d_ip":"127.0.0.1","d_port":6007,"d_user":"pgx","s_pwd":"pgx_pwd","d_dbname":"postgres_$$_public","d_table_name":"t17"}

6. send a meta/storage or computer cmd for node to run
{"job_type":"mysql_cmd","cmd":"./startmysql.sh 6001"}
{"job_type":"pgsql_cmd","cmd":"python2 start_pg.py port=5401"}
{"job_type":"cluster_cmd","cmd":"bash -x bin/cluster_mgr_safe --debug --pidfile=run.pid cluster_mgr.cnf"}

## http post file to node_mgr

1. send a file with paramemter for node_mgr to save
./http_post_file http://127.0.0.1:9998 "{\"file_type\":\"mysql\"}" ./mysql_shard1.json
./http_post_file http://127.0.0.1:9998 "{\"file_type\":\"pgsql\"}" ./postgresql_comp.json
./http_post_file http://127.0.0.1:9998 "{\"file_type\":\"cluster\"}" ./cluster_mgr.cnf



