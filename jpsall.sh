#! /bin/bash

for host in hadoop100 hadoop101 hadoop102
do
    echo "==========$host=========="
    ssh $host "jps"
done