#/bin/bash

while read -r line
do
echo $line
sleep 0.2
done < order.txt
