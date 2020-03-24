#!/bin/bash

gcloud compute --project chaturvedakash-amarendra firewall-rules create vpnrule5 --allow tcp:9005
gsutil cp gs://akash_cloud_bucket/reducer_cloud.py /home/chaturvedakash1/script.py
chmod 777 /home/chaturvedakash1/script.py
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
python3 get-pip.py
pip install --upgrade google-api-python-client
pip install oauth2client
python3 /home/chaturvedakash1/script.py 0 9005 &
