#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <script> <nodes file>"
    exit
fi

if [ ! -f $2 ]; then
    echo "Must enter a valid file"
    exit
fi
while read p; do
    ssh -o ConnectTimeout=10 -o StrictHostKeyChecking='no' ubc_eece411_2@$p $1 &
done <nodes.txt
