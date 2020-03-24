#!/bin/bash

gcloud compute --project chaturvedakash-amarendra firewall-rules create vpnrulemaster --allow tcp:8080
gsutil cp gs://akash_cloud_bucket/master_cloud_api.py /home/chaturvedakash1/script.py
gsutil cp gs://akash_cloud_bucket/google_cloud_api.py /home/chaturvedakash1/google_cloud_api.py
chmod 777 /home/chaturvedakash1/script.py
chmod 777 /home/chaturvedakash1/google_cloud_api.py
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
python3 get-pip.py
pip install --upgrade google-api-python-client
pip install oauth2client
pip install bs4
pip install beautifulsoup4
pip install requests
pip install textwrap3
python3 /home/chaturvedakash1/script.py &
