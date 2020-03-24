from xmlrpc.server import SimpleXMLRPCServer
import multiprocessing
import os,time
import re
import socket
from struct import pack,unpack
lock=multiprocessing.Lock()
import sys,json,logging
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
log_file="/home/chaturvedakash1/reducer_log.log"
logging.basicConfig(filename=log_file,filemode="w",format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time:%(asctime)s--%(message)s",level=logging.INFO)

if(sys.argv[1]):
    file_no=int(sys.argv[1])
else:
    logging.error("File number missing")

if(sys.argv[2]):
    port=int(sys.argv[2])
else:
    logging.error("Port missing")

def get_instance(compute,project,zone,instance):
    request = compute.instances().get(project=project, zone=zone, instance=instance)
    response = request.execute()
    network_interface=response["networkInterfaces"]
    network_interface_dict=network_interface[0]
    accessConfigs=network_interface_dict["accessConfigs"]
    accessConfigs_dict=accessConfigs[0]
    natIP=accessConfigs_dict["natIP"]
    return natIP

compute = discovery.build('compute', 'v1')
def get_ip(instance_name):
 try:
    host=get_instance(compute,'chaturvedakash-amarendra','us-central1-f',instance_name)
    if(host):
        return host
    else:
        logging.error("Rectriving ip address failed")
        exit(0)
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)


server_ip=get_ip('server-instance')
server_port=8001
word_count_reducer_ip='0.0.0.0'
word_count_reducer_port=port

server = SimpleXMLRPCServer((word_count_reducer_ip, word_count_reducer_port), logRequests=True,allow_none=True   )

def word_count_reduce(list):
 try:
    lock.acquire()
    exp=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    list=list.replace('(','')
    s=list.split(')')
    s=[x for x in s if x!='']
    words,unique_words=[],[]
    for i in s:
        st=i.split(':')
        words.append(st[0])
    words=[x for x in words if x!=[]]
    for i in words:
        if(i not in unique_words):
            unique_words.append(i)
    st=""
    for i in unique_words:
        st+="("+str(i)+":"+str(words.count(i))+")"
    command="set"+" "+str("word_count_reduce_result")+" "+str(len(st))+"\r\n"+str(st)+"\r\n"
    logging.debug("Command sent to server from reduce is %s"%str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    data=data.decode().strip()
    if(data=="STORED"):
        exp=True
    logging.debug("Data received from server in reduce")
    soc.close()
    lock.release()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return exp


def word_count_spool():
 try:
    res=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command="get"+" "+"word_count_shuffle_result"+"\r\n"
    logging.debug("Command sent to server from spool is %s"%str(command))
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
    d=data.splitlines()
    shuffled_input=[]
    for i in d:
        final=i.split(',')
        shuffled_input.append(final)

    shuffled_input[1]=[x for x in shuffled_input[1] if x!='']
    shuffle_input=shuffled_input[1][file_no]
    logging.debug("Input to reducer is %s"%str(shuffle_input))
    logging.info("Starting the reducer processes")
    res=word_count_reduce(shuffle_input)
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res


def inverted_index_reduce(list):
 try:
    lock.acquire()
    exp=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    logging.info("Reducer process id for {0}".format(os.getpid()))
    st=""
    st+=list
    command="set"+" "+str("inverted_index_reduce_result")+" "+str(len(st))+"\r\n"+str(st)+"\r\n"
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    logging.debug("Command sent to server from reduce is %s"%str(command))
    soc.send(command.encode())
    data=soc.recv(1400)
    data=data.decode()
    if(data.strip()=="STORED"):
        exp=True
    soc.close()
    lock.release()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return exp

def inverted_index_spool():
 try:
    res=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    command="get"+" "+"inverted_index_shuffle_result"+"\r\n"
    logging.debug("Command sent to server in spool is %s"%str(command))
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
    d=data.splitlines()
    reducer_input=d[1].split('*%*')
    reducer_input=reducer_input[file_no]
    logging.debug("The input to reducer is %s"%reducer_input)
    logging.info("Starting the reducer processes")
    res=inverted_index_reduce(reducer_input)
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res

def spool(reduce_fn):
  try:
    if(reduce_fn=="word_count_reduce"):
        res=word_count_spool()
    elif(reduce_fn=="inverted_index_reduce"):
        res=inverted_index_spool()
  except Exception as e:
     logging.error("Exception occurred", exc_info=True)
  return res


server.register_function(spool)
server.serve_forever()
