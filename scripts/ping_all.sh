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
    pingout=$(ping -c 2 -W 1 $p)
    unknown=""
    lost="100% packet loss"
    if [ "$pingout" == "" ] ; then
        :
    elif [ "${pingout/$lost}" != "$pingout" ] ; then
        echo "${p} 100% loss"
    else
        echo "${p} ok"
    fi
done <$1
