#!/bin/bash
#TODO - Implement parameters for number iterations and location of binary

if [ $1 -z ]; then
    echo "Must enter in a host:port"
    exit
fi

echo "Testing performance of 100 synchronous deletes..."
time(
for i in {1000..1100}; do
    OUTPUT="$(./kvclient remove $1 $i)"
done
)
