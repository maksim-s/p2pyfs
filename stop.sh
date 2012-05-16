#!/usr/bin/env bash

NUM_YFS=$1

if [ -z $NUM_YFS ]; then
    NUM_YFS=2
fi
export PATH=$PATH:/usr/local/bin

for ((i=0; i<$NUM_YFS; i++))
do
    YFS_DIR=$PWD"/yfs"$i
    UMOUNT="umount"
    if [ -f "/usr/local/bin/fusermount" -o -f "/usr/bin/fusermount" -o -f "/bin/fusermount" ]; then
	UMOUNT="fusermount -u";
    fi
    echo "$UMOUNT $YFS_DIR"
    $UMOUNT $YFS_DIR    
done


killall extent_server
killall yfs_client
killall lock_server
