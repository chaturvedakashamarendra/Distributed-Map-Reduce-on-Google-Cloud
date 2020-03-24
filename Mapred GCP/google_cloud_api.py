import argparse
import os
import time
from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

import googleapiclient.discovery
from six.moves import input


def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None

def create_instance(compute, project, zone, name, bucket,startup_script):
    # Get the latest Debian Jessie image.
    image_response = compute.images().getFromFamily(
        project='debian-cloud', family='debian-9').execute()
    source_disk_image = image_response['selfLink']

    machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    config = {
        'name': name,
        'machineType': machine_type,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                }
            }
        ],

        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/compute',
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script-url',
                #'value': "gs://akash_cloud_bucket/startup-script.sh"
                'value': startup_script
            }, {
                'key': 'bucket',
                'value': bucket
            }]
        }
    }

    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()

def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()



def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            #print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result['status']

        time.sleep(1)


def get_instance(compute,project,zone,instance):
    request = compute.instances().get(project=project, zone=zone, instance=instance)
    response = request.execute()
    network_interface=response["networkInterfaces"]
    network_interface_dict=network_interface[0]
    accessConfigs=network_interface_dict["accessConfigs"]
    accessConfigs_dict=accessConfigs[0]
    natIP=accessConfigs_dict["natIP"]
    return natIP


def stop_instance(compute,project,zone,instance):
    request = compute.instances().stop(project=project, zone=zone, instance=instance)
    response = request.execute()
    pprint(response)
    return response

def start_instance(compute,project,zone,instance):
    request = compute.instances().start(project=project, zone=zone, instance=instance)
    response = request.execute()
    pprint(response)
    return response

