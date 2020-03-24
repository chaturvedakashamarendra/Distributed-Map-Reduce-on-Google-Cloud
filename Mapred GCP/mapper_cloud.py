from xmlrpc.server import SimpleXMLRPCServer
import multiprocessing
from multiprocessing import Value
import os,time,logging
import socket
from struct import pack,unpack
import re,sys
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
log_file="/home/chaturvedakash1/map_log.log"
logging.basicConfig(filename=log_file,filemode='w',format="Filename : %(filename)s--Line number: %(lineno)d--Process is: %(process)d--Time: %(asctime)s--%(message)s",level=logging.INFO)

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

lock=multiprocessing.Lock()
server = SimpleXMLRPCServer(('0.0.0.0',port), logRequests=True,allow_none=True)

def get_data(map_fn):
 try:
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    if map_fn=="word_count_map":
        command="get"+" "+"word_count_map_input"+"\r\n"
    elif map_fn=="inverted_index_map":
        command="get"+" "+"inverted_index_map_input"+"\r\n"
    logging.info('Command sent to server is %s',str(command))
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
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return data

def word_count_map(n):
 try:
    lock.acquire()
    exp=False
    logging.info(server_ip)
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    st=""
    s=re.findall(r'\w+',n)
    for i in s:
        st=st+"("+str(i)+":"+"1"+")"+","
    command="set"+" "+str("word_count_map_result")+" "+str(len(st))+"\r\n"+str(st)+"\r\n"
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    logging.debug("Command sent to server from map is %s"%str(command))
    soc.send(command.encode())
    data=soc.recv(1400)
    d=data.decode()
    d=d.strip()
    if(d=="STORED"):
        exp=True
    soc.close()
    lock.release()
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return exp



def word_count_spool():
 try:
    res=False
    logging.info("request to word count map")
    data=get_data("word_count_map")
    l=data.splitlines()
    map_input=l[1].split(':;')
    map_input.pop()
    map_input=map_input[file_no]
    logging.info('Data received from the server')
    logging.info('Starting the word count mappers')
    res=word_count_map(map_input)
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return res


def inverted_index_map(s):
 try:
    lock.acquire()
    exp=False
    host=server_ip
    port=server_port
    soc=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    soc.connect((host,port))
    logging.info("Mapper process id for {0}".format(os.getpid()))
    words=re.findall(r'\w+',s)
    unique_words=[]
    for i in words:
        if(i not in unique_words):
            unique_words.append(i)
    doc="doc_"+str(file_no)
    st=""
    for i in unique_words:
        st+="("+str(i)+":"+str(doc)+"|"+str(words.count(i))+")"
    command="set"+" "+str("inverted_index_map_result")+" "+str(len(st))+"\r\n"+str(st)+"\r\n"
    logging.debug("Command sent to server from map is %s"%str(command))
    length=pack('>Q',len(command.encode()))
    soc.send(length)
    soc.send(command.encode())
    data=soc.recv(1400)
    d=data.decode()
    if(d.strip()=="STORED"):
        exp=True
    soc.close()
    lock.release()
    logging.debug("Data received from server in map")
 except Exception as e:
     logging.error("Exception occurred", exc_info=True)
 return exp



def inverted_index_spool():
  try:
    res=False
    data=get_data("inverted_index_map")
    l=data.splitlines()
    map_input=l[1].split(':;')
    map_input.pop()
    map_input=map_input[file_no]
    logging.debug('The input to mapper is %s'%str(map_input))
    logging.info('Starting mapper processes')
    res=inverted_index_map(map_input)
  except Exception as e:
      logging.error("Exception occurred", exc_info=True)
  return res




def spool(map_fn):
  try:
    if map_fn=="word_count_map":
        res=word_count_spool()
    elif map_fn=="inverted_index_map":
        res=inverted_index_spool()
  except Exception as e:
     logging.error("Exception occurred", exc_info=True)
  return res


server.register_function(spool)
server.serve_forever()

