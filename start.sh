#!/usr/bin/env bash

ulimit -c unlimited

LOSSY=$1
NUM_LS=$2
NUM_YFS=$3

if [ -z $NUM_LS ]; then
    NUM_LS=0
fi

if [ -z $NUM_YFS ]; then
    NUM_YFS=2
fi

BASE_PORT=$RANDOM
BASE_PORT=$[BASE_PORT+2000]
EXTENT_PORT=$BASE_PORT


echo $NUM_YFS
for ((i=0; i<$NUM_YFS; i++))
do
    
    #echo $i
    YFS_PORTS[$i]=$[BASE_PORT+2*$i]
    #echo ${YFS_PORTS[i]}
    YFS_DIRS[$i]=$PWD"/yfs"$i
    #echo ${YFS_DIRS[i]}
done


#YFS1_PORT=$[BASE_PORT+2]
#YFS2_PORT=$[BASE_PORT+4]

LOCK_PORT=$[BASE_PORT+$NUM_YFS*2+2]

YFSDIR1=$PWD/yfs1
YFSDIR2=$PWD/yfs2

if [ "$LOSSY" ]; then
    export RPC_LOSSY=$LOSSY
fi

if [ $NUM_LS -gt 1 ]; then
    x=0
    rm config
    while [ $x -lt $NUM_LS ]; do
      port=$[LOCK_PORT+2*x]
      x=$[x+1]
      echo $port >> config
    done
    x=0
    while [ $x -lt $NUM_LS ]; do
      port=$[LOCK_PORT+2*x]
      x=$[x+1]
      echo "starting ./lock_server $LOCK_PORT $port $EXTENT_PORT > lock_server$x.log 2>&1 &"
      ./lock_server $LOCK_PORT $port $EXTENT_PORT > lock_server$x.log 2>&1 &
      sleep 1
    done
else
    echo "starting ./lock_server $LOCK_PORT> lock_server.log 2>&1 &"
    ./lock_server $LOCK_PORT $LOCK_PORT $EXTENT_PORT > lock_server.log 2>&1 &
    sleep 1
fi

unset RPC_LOSSY

echo "starting ./extent_server $EXTENT_PORT > extent_server.log 2>&1 &"
./extent_server $EXTENT_PORT > extent_server.log 2>&1 &
sleep 1

count=0
for YFSDIR in ${YFS_DIRS[@]}
do
    rm -rf $YFSDIR
    mkdir $YFSDIR || exit 1
    sleep 1
    echo "starting ./yfs_client $YFSDIR $EXTENT_PORT $LOCK_PORT > yfs_client$count.log 2>&1 &"
    ./yfs_client $YFSDIR $EXTENT_PORT $LOCK_PORT > yfs_client$count.log 2>&1 &
    sleep 1
    count=$(( $count + 1 ))
done

sleep 1

# make sure FUSE is mounted where we expect

x=0
for YFSDIR in ${YFS_DIRS[@]}
do
    echo "Checking $YFSDIR"
    pwd=`pwd -P`
    if [ `mount | grep "$pwd/yfs$x" | grep -v grep | wc -l` -ne 1 ]; then
	./stop.sh
	echo "Failed to mount YFS properly at ./yfs$x"
	exit -1
    fi
    x=$(( $x + 1 ))
done

