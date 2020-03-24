#!/bin/bash

read -p "Enter 1 to initiate word count or 2 to initiate inverted index map reduce task: "  task
read -p "Enter the mapper instance ip: " master_ip
if [ $task == "1" ]; then
    echo "hi" $1
    curl -v http://$master_ip:8080/ -F datafile=@configuration_word_count.json -o word_count_output.txt
    echo "Refer word_count_output.txt for output of map reduce task"
else
    curl -v http://$master_ip:8080/ -F datafile=@configuration_inverted_index.json -o inverted_index_output.txt
    echo "Refer inverted_index_output.txt for output of map reduce task"
fi
