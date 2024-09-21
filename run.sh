#!/bin/bash

if [ "$#" -ne 1 ]; then
    exit 1
fi

NUM_REQUESTS=$1

declare -a ADDRESSES

AVAILABLE_NODES=($( /share/ifi/available-nodes.sh ))
MAX_NODES=${#AVAILABLE_NODES[@]}

for (( i=0; i<$NUM_REQUESTS; i++ )); do

    NODE_INDEX=$(( i % MAX_NODES ))
    NODE=${AVAILABLE_NODES[$NODE_INDEX]}
    PORT=$(shuf -i 49152-65535 -n1)
    ADDRESSES+=("$NODE:$PORT")
done

ADDRESSES_STR=$(IFS=, ; echo "${ADDRESSES[*]}")

for (( i=0; i<$NUM_REQUESTS; i++ )); do
    NODE_INDEX=$(( i % MAX_NODES ))
    NODE=${AVAILABLE_NODES[$NODE_INDEX]}
    PORT=${ADDRESSES[$i]#*:}
    ssh $NODE "pip3 install flask" > /dev/null
    ssh $NODE "pip3 install waitress" > /dev/null
    ssh -f $NODE "python3 $PWD/server.py $NODE $PORT $ADDRESSES_STR"
done

#echo "Addresses ${ADDRESSES[0]} ${ADDRESSES[*]}"


for i in "${!ADDRESSES[@]}"; do
    ADDRESSES[$i]="\"${ADDRESSES[$i]}\""
done

echo "[${ADDRESSES[*]}]" | sed 's/ /, /g'
