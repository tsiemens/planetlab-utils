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
    scp config.json ubc_eece411_2@$p: &
done <$1
