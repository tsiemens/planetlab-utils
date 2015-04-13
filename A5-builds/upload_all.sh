#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <nodes file>"
    exit
fi

if [ ! -f $1 ]; then
    echo "Must enter a valid file"
    exit
fi
while read p; do
    ssh -o ConnectTimeout=10 -o StrictHostKeyChecking='no' ubc_eece411_2@$p "curl -L -o kvserver https://www.dropbox.com/s/qow307ga3v4gy5f/kvserver?dl=0" &
done <$1
