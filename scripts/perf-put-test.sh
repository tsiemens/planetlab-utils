#!/bin/bash
#TODO - Implement parameters for number iterations and location of binary

if [ $1 -z ]; then
    echo "Must enter in a host:port"
    exit
fi

echo "Testing performance of 100 synchronous puts..."
time(
for i in {1000..1100}; do
    OUTPUT="$(./kvclient put $1 $i 'hello' )"
done
)
