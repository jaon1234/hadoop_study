# /bin/bash

if [ $# -lt 1 ]
then
    echo "No Args input ..."
    exit;
fi

case $1 in
"start")
    echo $1
    echo "=============启动hadoop集群============="
    echo "=============启动hdfs============="
    ssh hadoop100 "/opt/modules/hadoop-3.2.4/sbin/start-dfs.sh"
    echo "=============启动yarn============="
    ssh hadoop101 "/opt/modules/hadoop-3.2.4/sbin/start-yarn.sh"
    echo "=============启动historyserver============="
    ssh hadoop100 "/opt/modules/hadoop-3.2.4/bin/mapred --daemon start historyserver"
;;
"stop")
    echo $1
    echo "=============关闭hadoop集群============="
    echo "=============关闭historyserver============="
    ssh hadoop100 "/opt/modules/hadoop-3.2.4/bin/mapred --daemon stop historyserver"
    echo "=============关闭yarn============="
    ssh hadoop101 "/opt/modules/hadoop-3.2.4/sbin/stop-yarn.sh"
    echo "=============关闭hdfs============="
    ssh hadoop100 "/opt/modules/hadoop-3.2.4/sbin/stop-dfs.sh"
;;
*)
    echo $1
    echo "input args error..."
;;
esac