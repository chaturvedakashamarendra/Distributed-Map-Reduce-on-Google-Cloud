
import requests,math,re,sys,logging
import textwrap
from bs4 import BeautifulSoup
import socket
import time
import multiprocessing
import os
from struct import pack,unpack
import re
import json
import xmlrpc.client as xc
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import google_cloud_api as gc
import cgi
import http.server
import io
from http.server import BaseHTTPRequestHandler
import time,json

task_completed=False
log_file="/home/chaturvedakash1/master_log.log"
logging.basicConfig(filename=log_file,filemode="w",format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s",level=logging.INFO)


def validate(configurations_list,msg=""):
    exp=True
    if(configurations_list[0]==""):
        logging.error('Input data is missing')
        msg="Input data is missing"
        exp=False
    if (configurations_list[1] not in ["word_count_map","inverted_index_map"]):
        logging.error('Map functionis not supported')
        msg="Map function is not supported"
        exp=False
    if(configurations_list[2] not in ["word_count_reduce","inverted_index_reduce"]):
        logging.error('Reduce function is not supported')
        msg="Reduce function is not supported"
        exp=False
    if(configurations_list[3]==""):
        logging.error('Output location is missing')
        msg="Output location is missing"
        exp=False
    return exp,msg


configurations_list=[]
class PostHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        # Parse the form data posted
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                'REQUEST_METHOD': 'POST',
                'CONTENT_TYPE': self.headers['Content-Type'],
            }
        )
        out = io.TextIOWrapper(
            self.wfile,
            encoding='utf-8',
            line_buffering=False,
            write_through=True,
        )
        print(self.rfile)
        for field in form.keys():
            field_item = form[field]
            if field_item.filename:
                # The field contains an uploaded file
                file_data = field_item.file.read()
                file_len = len(file_data)
                print(file_len)
                file_data=file_data.decode()
                json_data=json.loads(file_data)
                input_data=json_data['input_data']
                configurations_list.append(input_data)
                map_fn=json_data['map_fn']
                configurations_list.append(map_fn)
                reduce_fn=json_data['reduce_fn']
                configurations_list.append(reduce_fn)
                output_file=json_data['output_location']
                configurations_list.append(output_file)
                msg=""
                res,msg=validate(configurations_list,msg)
                if(res):
                    result=mapReduce(input_data,map_fn,reduce_fn,output_file)
                    if(result):
                        try:
                            with open(json_data['output_location']) as f:
                                output=f.read()
                        except Exception as e:
                            logging.error("Exception occurred", exc_info=True)
                            self.send_response(200)
                            self.send_header('Content-Type',
                             'text/plain; charset=utf-8')
                            self.end_headers()
                            out.write(
                                'Output is {}\n'.format(
                                "Output file does not exist"))
                            out.detach()

                        self.send_response(200)
                        #self.send_response("Done")
                        self.send_header('Content-Type',
                             'text/plain; charset=utf-8')
                        self.end_headers()
                        out.write(
                            'Map Reduce task is {}\n{}\n'.format(
                            "successful",output))
                        out.detach()
                    else:
                        self.send_response(200)
                        self.send_header('Content-Type',
                             'text/plain; charset=utf-8')
                        self.end_headers()
                        out.write(
                            'Mapreduce task {}\n'.format(
                            "failed"))
                        out.detach()
                else:
                    logging.error("Map reduce initialization failed")
                    self.send_response(200)
                    self.send_header('Content-Type',
                             'text/plain; charset=utf-8')
                    self.end_headers()
                    out.write('Mapreduce task failed because \n{}\n'.format(msg))
                    out.detach()
            else:
                logging.error("Configuration File not passed by client")



compute = discovery.build('compute', 'v1')
project, bucket, zone='chaturvedakash-amarendra','akash_cloud_storage','us-central1-f'

def get_ip(instance_name):
 try:
    host=gc.get_instance(compute,'chaturvedakash-amarendra','us-central1-f',instance_name)
    if(host):
        return host
    else:
        logging.error("Rectriving ip address failed")
        exit(0)
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)


server_ip=get_ip('server-instance')
server_port=8001

def map_input(file,map_fn):
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    s=""
    for i in file:
        s+=i+":;"
    if(map_fn=="word_count_map"):
        command="set"+" "+str("word_count_map_input")+" "+str(len(s))+"\r\n"+str(s)+"\r\n"
    elif(map_fn=="inverted_index_map"):
        command="set"+" "+str("inverted_index_map_input")+" "+str(len(s))+"\r\n"+str(s)+"\r\n"
    logging.info('command being passed to server in "map_input" is %s',str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    result=data.decode()
    logging.info('Response received from server in "map_function" is %s',str(result))
    soc.close()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return result



def shuffle(map_fn):
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command=""
    if(map_fn=="word_count_map"):
        command="shuffle"+" "+str("word_count_map_result")+"\r\n"
    elif(map_fn=="inverted_index_map"):
        command="shuffle"+" "+str("inverted_index_map_result")+"\r\n"
    length=pack('>Q',len(command))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    result=data.decode()
    soc.close()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return result

def store_output_file(reduce_fn,output_location):
 res=True
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command=""
    if(reduce_fn=="word_count_reduce"):
        command="get"+" "+"word_count_reduce_result"+"\r\n"
    elif(reduce_fn=="inverted_index_reduce"):
        command="get"+" "+"inverted_index_reduce_result"+"\r\n"
    logging.info('Command sent to server in store_output_file is %s',str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    bs=soc.recv(8)
    (length,)=unpack('>Q',bs)
    data=b''
    while len(data)< length:
        to_Read=length-len(data)
        data+=soc.recv(1024 if to_Read > 1024 else to_Read)
    data=str(data.decode())
    soc.close()
    if(reduce_fn=="word_count_reduce"):
        output="The output of word count map reduce task is "+"\n"
    elif(reduce_fn=="inverted_index_reduce"):
        output="The output of inverted index map reduce task is "+"\n"
        data=data.replace("::","  ")
        data=data.replace(":",",")
    result=data.splitlines()
    list=result[1].replace('(','')
    s=list.split(')')
    for i in s:
        output+=str(i)+"\n"
    file=open(output_location,"w+")
    file.write(output)
    file.close()
 except Exception as e:
     res=False
     logging.error("Exception occurred", exc_info=True)
 return res

def backup():
 try:
    res=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command="backup"+"\r\n"
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1024)
    data=str(data.decode())
    soc.close()
    if(data.strip()=="DONE"):
        res=True
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res


def create_instances(function):
  try:
    bucket_location="gs://akash_cloud_bucket/"
    exp=False
    instance_ips=[]
    if(function=="map"):
        operation_1 = gc.create_instance(compute, project, zone,'mapper-1', bucket,bucket_location+'map-1-startup_script.sh')
        result_1=gc.wait_for_operation(compute, project, zone, operation_1['name'])
        if(result_1.strip()=="DONE"):
            mapper_1_ip=get_ip('mapper-1')
            instance_ips.append(mapper_1_ip)
        else:
            logging.error("Mapper instance 1 creation failed")
            exit(0)
        operation_2 = gc.create_instance(compute, project, zone,'mapper-2', bucket,bucket_location+'map-2-startup_script.sh')
        result_2=gc.wait_for_operation(compute, project, zone, operation_2['name'])
        if(result_2.strip()=="DONE"):
            mapper_2_ip=get_ip('mapper-2')
            instance_ips.append(mapper_2_ip)
        else:
            logging.error("Mapper instance 2 creation failed")
            exit(0)
        operation_3 = gc.create_instance(compute, project, zone,'mapper-3', bucket,bucket_location+'map-3-startup_script.sh')
        result_3=gc.wait_for_operation(compute, project, zone, operation_3['name'])
        if(result_3.strip()=="DONE"):
            mapper_3_ip=get_ip('mapper-3')
            instance_ips.append(mapper_3_ip)
        else:
            logging.error("Mapper instance 3 creation failed")
            exit(0)
    elif(function=="reduce"):
        operation_1 = gc.create_instance(compute, project, zone,'reducer-1', bucket,bucket_location+'reduce-1-startup_script.sh')
        result_1=gc.wait_for_operation(compute, project, zone, operation_1['name'])
        if(result_1.strip()=="DONE"):
            reducer_1_ip=get_ip('reducer-1')
            instance_ips.append(reducer_1_ip)
        else:
            logging.error("reducer instance 1 creation failed")
            exit(0)
        operation_2 = gc.create_instance(compute, project, zone,'reducer-2', bucket,bucket_location+'reduce-2-startup_script.sh')
        result_2=gc.wait_for_operation(compute, project, zone, operation_2['name'])
        if(result_2.strip()=="DONE"):
            reducer_2_ip=get_ip('reducer-2')
            instance_ips.append(reducer_2_ip)
        else:
            logging.error("reducer instance 2 creation failed")
            exit(0)
        operation_3 = gc.create_instance(compute, project, zone,'reducer-3', bucket,bucket_location+'reduce-3-startup_script.sh')
        result_3=gc.wait_for_operation(compute, project, zone, operation_3['name'])
        if(result_3.strip()=="DONE"):
            reducer_3_ip=get_ip('reducer-3')
            instance_ips.append(reducer_3_ip)
        else:
            logging.error("reducer instance 3 creation failed")
            exit(0)
    if(len(instance_ips)==3):
        return True,instance_ips
    else:
        return False,None
  except Exception as e:
     logging.error("Exception occurred", exc_info=True)

def terminate_instances(function):
  try:
    if(function=="map"):
        operation_1 = gc.delete_instance(compute, project, zone, 'mapper-1')
        result_1=gc.wait_for_operation(compute, project, zone, operation_1['name'])
        if(result_1.strip()=="DONE"):
            logging.info("Mapper 1 terminated")
            status_1=True
        else:
            logging.error("Mapper 1 termination failed")
        operation_2 = gc.delete_instance(compute, project, zone, 'mapper-2')
        result_2=gc.wait_for_operation(compute, project, zone, operation_2['name'])
        if(result_2.strip()=="DONE"):
            logging.info("Mapper 2 terminated")
            status_2=True
        else:
            logging.error("Mapper 2 termination failed")
        operation_3 = gc.delete_instance(compute, project, zone, 'mapper-3')
        result_3=gc.wait_for_operation(compute, project, zone, operation_3['name'])
        if(result_3.strip()=="DONE"):
            logging.info("Mapper 3 terminated")
            status_3=True
        else:
            logging.error("Mapper 3 termination failed")
    elif(function=="reduce"):
        operation_1 = gc.delete_instance(compute, project, zone, 'reducer-1')
        result_1=gc.wait_for_operation(compute, project, zone, operation_1['name'])
        if(result_1.strip()=="DONE"):
            logging.info("reducer 1 terminated")
            status_1=True
        else:
            logging.error("reducer 1 termination failed")
        operation_2 = gc.delete_instance(compute, project, zone, 'reducer-2')
        result_2=gc.wait_for_operation(compute, project, zone, operation_2['name'])
        if(result_2.strip()=="DONE"):
            logging.info("reducer 2 terminated")
            status_2=True
        else:
            logging.error("reducer 2 termination failed")
        operation_3 = gc.delete_instance(compute, project, zone, 'reducer-3')
        result_3=gc.wait_for_operation(compute, project, zone, operation_3['name'])
        if(result_3.strip()=="DONE"):
            logging.info("reducer 3 terminated")
            status_3=True
        else:
            logging.error("reducer 3 termination failed")
    if(status_1 and status_2 and status_3):
        return True
    else:
        return False
  except Exception as e:
     logging.error("Exception occurred", exc_info=True)

def mapReduce(input_data,map_fn,reduce_fn,output_location):
    try:
        files=[]
        result=""
        if(map_fn=="word_count_map"):
            response = requests.get(input_data)
            soup=BeautifulSoup(response.text,'html.parser')
            logging.info('Parsing the input html file %s',input_data)
            s=""
            count=0
            for link in soup.find_all('p'):
                s+=link.text
            s_len=len(s)
            length=math.ceil(s_len//3)
            file_input=textwrap.wrap(s,length)
            print(file_input)
            print(len(file_input))
            logging.debug('Calling map_input to store input data in key-value store')
            result=map_input(file_input,map_fn)

        elif(map_fn=="inverted_index_map"):
            for i in input_data:
                response = requests.get(i)
                soup=BeautifulSoup(response.text,'html.parser')
                s=""
                for i in soup.find_all('p'):
                    s+=i.text
                s=textwrap.fill(s)
                files.append(s)
            result=map_input(files,map_fn)
        if(result.strip()=="STORED"):
            if(map_fn=="word_count_map"):
                creation_status,mapper_ips=create_instances("map")
                logging.info(creation_status)
                if(creation_status):
                    time.sleep(80)
                    logging.info('RPC call to word_count_mapper')
                    logging.info(str(mapper_ips[0]))
                    proxy_1 = xc.ServerProxy("http://%s:%s"%(str(mapper_ips[0]),str(9000)),allow_none=True)
                    map_result_1=proxy_1.spool(map_fn)
                    print(map_result_1)
                    proxy_2 = xc.ServerProxy("http://%s:%s"%(str(mapper_ips[1]),str(9001)),allow_none=True)
                    map_result_2=proxy_2.spool(map_fn)
                    print(map_result_2)
                    proxy_3 = xc.ServerProxy("http://%s:%s"%(str(mapper_ips[2]),str(9002)),allow_none=True)
                    map_result_3=proxy_3.spool(map_fn)
                    print(map_result_3)
                else:
                    logging.error("Mapper instance creation failed")
                    exit(0)
            elif(map_fn=="inverted_index_map"):
                creation_status,mapper_ips=create_instances("map")
                if(creation_status):
                    time.sleep(80)
                    logging.info('RPC call to inverted_index_mapper')
                    proxy_1 = xc.ServerProxy("http://%s:%s"%(str(mapper_ips[0]),str(9000)),allow_none=True)
                    map_result_1=proxy_1.spool(map_fn)
                    print(map_result_1)
                    proxy_2 = xc.ServerProxy("http://%s:%s"%(str(mapper_ips[1]),str(9001)),allow_none=True)
                    map_result_2=proxy_2.spool(map_fn)
                    print(map_result_2)
                    proxy_3 = xc.ServerProxy("http://%s:%s"%(str(mapper_ips[2]),str(9002)),allow_none=True)
                    map_result_3=proxy_3.spool(map_fn)
                    print(map_result_3)
                else:
                    logging.error("Mapper instance creation failed")
                    exit(0)
            if(map_result_1 and map_result_2 and map_result_3):
                result=terminate_instances("map")
                if(result):
                    logging.info("Termination of mapper instances successful")
                else:
                    logging.error("Instances termination failed")
                logging.info('Map task successfully completed')
                logging.debug('Calling shuffle in master')
                shuffle_result=shuffle(map_fn)
                logging.info('The response of shuffle in master is %s',str(shuffle_result))
                shuffle_result=shuffle_result.strip()
                if(shuffle_result=="STORED"):
                    logging.info('Shuffle task successfully completed')
                    if(reduce_fn=="word_count_reduce"):
                        creation_status,reducer_ips=create_instances("reduce")
                        if(creation_status):
                            time.sleep(80)
                            logging.info('RPC call to word_count reducer')
                            proxy = xc.ServerProxy("http://%s:%s"%(str(reducer_ips[0]),str(9005)),allow_none=True)
                            reducer_result_1=proxy.spool(reduce_fn)
                            print(reducer_result_1)
                            proxy = xc.ServerProxy("http://%s:%s"%(str(reducer_ips[1]),str(9006)),allow_none=True)
                            reducer_result_2=proxy.spool(reduce_fn)
                            print(reducer_result_2)
                            proxy = xc.ServerProxy("http://%s:%s"%(str(reducer_ips[2]),str(9007)),allow_none=True)
                            reducer_result_3=proxy.spool(reduce_fn)
                            print(reducer_result_3)
                        else:
                            logging.error("Reducer instance creation failed")
                            exit(0)
                    elif(reduce_fn=="inverted_index_reduce"):
                        creation_status,reducer_ips=create_instances("reduce")
                        if(creation_status):
                            time.sleep(80)
                            logging.info('RPC call to inverted_index_reducer')
                            proxy_1 = xc.ServerProxy("http://%s:%s"%(str(reducer_ips[0]),str(9005)),allow_none=True)
                            reducer_result_1=proxy_1.spool(reduce_fn)
                            proxy_2 = xc.ServerProxy("http://%s:%s"%(str(reducer_ips[1]),str(9006)),allow_none=True)
                            reducer_result_2=proxy_2.spool(reduce_fn)
                            proxy_3 = xc.ServerProxy("http://%s:%s"%(str(reducer_ips[2]),str(9007)),allow_none=True)
                            reducer_result_3=proxy_3.spool(reduce_fn)
                            print(reducer_result_1)
                            print(reducer_result_2)
                            print(reducer_result_3)
                        else:
                            logging.error("Reducer instance creation failed")
                            exit(0)
                    if(reducer_result_1 and reducer_result_2 and reducer_result_3):
                        result=terminate_instances("reduce")
                        if(result):
                            logging.info("Termination of reducer instances successful")
                        else:
                            logging.error("Instances termination failed")
                        logging.info('Reduce task successfully completed')
                        res=store_output_file(reduce_fn,output_location)
                        if(res):
                            logging.info('The output of the map reduce task has been stored in the file %s',output_location)
                            res=backup()
                            if(res):
                                logging.info('Map reduce task successfully completed')
                                print("Map reduce task successfully completed")
                                task_completed=True
                            else:
                                logging.error("Backup failed")
                    else:
                        logging.error('Reducer task %s'% str(reduce_fn)+'failed')
            else:
                logging.error('Mapper task %s'% str(map_fn)+'failed')

        else:
            logging.error('Storing input data in server failed')

    except Exception as e:
      logging.error("Exception occurred", exc_info=True)
    return task_completed

if __name__ == '__main__':
    from http.server import HTTPServer
    server = HTTPServer(('0.0.0.0', 8080), PostHandler)
    print('Starting server, use <Ctrl-C> to stop')
    server.serve_forever()
