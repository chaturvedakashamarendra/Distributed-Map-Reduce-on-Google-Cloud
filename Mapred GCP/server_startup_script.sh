#!/bin/bash

#gcloud compute --project chaturvedakash-amarendra firewall-rules create server_vpn_rule --allow tcp:8001
gcloud compute --project chaturvedakash-amarendra firewall-rules create vpnrule1 --allow tcp:8001
gsutil cp gs://akash_cloud_bucket/server_cloud.py /home/chaturvedakash1/script.py
chmod 777 /home/chaturvedakash1/script.py


python3 /home/chaturvedakash1/script.py &

