#client module for SUFS

import socket
#import click
import re
import rpyc 
import pickle

from urllib.request import build_opener 
from S3Handler import S3Handler 


from boto.s3.connection import S3Connection
from boto.s3.key import Key
from reply import Reply 
from io import BytesIO 
import boto3
import os 

file_pattern = re.compile('^\\(.+\\)*(.+)\.(.+)$')
block_size = 134217728
#data_node_IP = '' 
name_node_IP = '54.202.98.249'
#node_IPs = ['ip1','ip2']



#create will allow the user to create a file 
#it will then tell the user if it was not successful  
def make_file(bucket_name, file_name, to_path):
    s3 = boto3.resource('s3')#, aws_access_key_id=ACCESS_ID, aws_secret_access_key=ACCESS_KEY)
    s3data = s3.Object(bucket_name=bucket_name, key=file_name).get()

    file_size = s3data['ContentLength']
    print(file_size)

    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

 
    name_reply = name_node.make_file(file_size, to_path)

    print(file_size) 
    print(name_reply)

    #block_locations = name_reply.result
    block_locations = name_reply

    send_blocks = [] 
    #j = 1 
 
    #with open(file_path, 'rb+') as input: 
    #    block = []
    #    bytes = input.read()
    
    for i in range(0, len(block_locations), 2):  
        node_IPs = block_locations[i+1].split('{')[1]
        node_IPs = node_IPs.split('}')[0]
        node_IPs = node_IPs.split(',')
        send_blocks.append([block_locations[i].replace("/","!@!"), node_IPs])

    i = 0 
    try:
        for chunk in iter(lambda: s3data['Body'].read(block_size), b''):
            block_id = send_blocks[i][0]
            print(send_blocks[i][1][0])
            data_conn = rpyc.connect(send_blocks[i][1][0], 5000, config = {'allow_public_attrs': True})
            data_node = data_conn.root 
            #print("Now inserting: ", block_id)
            data_reply = Reply.Load(data_node.put_block(block_id, chunk, send_blocks[i][1]))
            print(data_reply.status)
            i+=1
    except:
        pass  


#delete will allow the user to delete a file given a full file path 
def delete_file(path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    name_reply = name_node.delete_path(path)
    
    delete_result = name_reply
    if (delete_result != 1): 
        print("error deleting file")


#delete will allow the user to read a file given a full file path 
#it will ask NameNode to delete
#it will then tell the user whether or not the delete was successful
#if the file does not exist, it will tell the user
def read_file(path, file_name): 

    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    block_locations = []
    name_reply = name_node.read_file(path, block_locations)
    #block_locations = name_reply
    print(block_locations)

    if name_reply is 0: 
        print("File could not be located")
        return  

    for i in range(len(block_locations)):
        node_IPs = block_locations[i].split(",")
        name = block_locations[i][0]
        node_IPs = block_locations[i][1].split('{')
        node_IPs = node_IPs.split('}')[0]
        node_IPs = node_IPs.split(',')
        send_blocks.append([block_locations[i].replace("/","!@!"), node_IPs])

    fetched_bytes = BytesIO()
    fetched_bytes.write(name_reply)

    fetched_bytes.flush()
    fetched_bytes.seek(0)

    #temp = block_locations.split(',')
    receive_blocks = [] 
    i = 0 
    '''
    for i in range(0, len(block_locations), 2): 
        receive_blocks.append([block_locations[i]])
        node_IPs = block_locations[i+1].split('{')[1]
        node_IPs = node_IPs.split('}')[0]
        node_IPs = node_IPs.split(',')
        receive_blocks[i][0].append(node_IPs)
    '''
    for block in receive_blocks:
        data_conn = rpyc.connect(block, 5000, config = {'allow_public_attrs': True})
        data_node = data_conn.root
        data_reply = Reply.Load(data_node.get_block(block))
        fetched_bytes = BytesIO()
        fetched_bytes.write(data_reply.result)

        fetched_bytes.flush()
        fetched_bytes.seek(0)
        if not data_reply.error:
            with open(file_name, 'ab+') as dest: 
                shutil.copyfileobj(fetched_bytes, dest, blocksize)
    
#mkdir will allow the user to make a directory in SUFS
#it will ask the NameNode to create the path 
#the user will be notified if the path could not be created
def make_dir(path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    name_reply = name_node.create_directory(path)
    print(name_reply)

    if not name_reply:
        print("Could not make directory") 
        


#rmdir will allow the user to remove a directory in SUFS if it is empty 
#it will ask the NameNode to delete the path 
#the user will be notified if the delete was not successful 
def delete_dir(path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    name_reply = name_node.delete_path(path)

    if not name_reply:
        print("Could not remove directory")

#lsdir will allow the user to list the contents of a directory if it exists 
#it will ask the NameNode for the names of the contents in the directory 
#the user will see a listing of the contents if the directory exists
def list_dir(path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    content_names = [] 
    name_reply = name_node.list_directory(path, content_names)

    if name_reply: 
        for name in content_names: 
            print(name)
    else: 
        print("Could not list directory contents")

#lsdata will allow the user to list the data blocks of a file given a file path  
#it will ask the NameNode for the data blocks which store the contents of the file  
#the user will see a listing of the blocks if the file exists     
def list_data_nodes(path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    block_locations = []
    name_reply = name_node.read_file(path, block_locations)

    if not name_reply:
        for block in block_locations: 
            print(block[1])
    else: 
        print("Data blocks could not be listed")


def call_function(command): 
    if(command[0] == 'upload'):
        if len(command) != 4: 
            print('error: wrong number args')
        else: 
            make_file(command[1], command[2], command[3]) 
    if(command[0] == 'rm'):
        if len(command) != 2: 
            print('error: wrong number args')
        else: 
            delete_file(command[1]) 
    if(command[0] == 'ls'): 
        list_dir(command[1]) 
    if(command[0] == 'download'):
        if len(command) != 3: 
            print('error: wrong number args')
        else: 
            read_file(command[1], command[2])
    if(command[0] == 'mkdir'): 
        if len(command) != 2: 
            print('error: wrong number args')
        else: 
            make_dir(command[1])
    if(command[0] == 'rmdir'):
        if len(command) != 2: 
            print('error: wrong number args')
        else: 
            delete_dir(command[1])
    if(command[0] == 'lsdata'): 
        if len(command) != 2: 
            print('error: wrong number args')
        else: 
            list_data_nodes(command[1])
    if(command[0] == 'help'): 
        print('List of commands: upload, rm, ls, download, mkdir, rmdir, lsdata')
        print('Usage:')
        print('upload <s3 bucket name> <file name> <SUFS path name>')
        print('upload test-files-project 1GB.bin hahahapleasework.bin')
        print('rm <SUFS path name>')
        print('rm hahahapleasework.bin')
        print('ls <SUFS path name>')
        print('ls coolestdomainintheuniverse')
        print('download <SUFS path name> <file name>')
        print('download hahahapleasework.bin 1GB.bin')
        print('mkdir <SUFS dir name>')
        print('rmdir <SUFS dir name>')
        print('rmdir <SUFS path name>')
        


def main(): 
    cont = True 
    print("You have entered the SUFS interactive shell program")
    while(cont): 
        command_list = ['upload', 'rm', 'ls', 'download', 'mkdir', 'rmdir', 'lsdata', 'quit', 'help']
        command = []
        command.append(input('>> '))

        if ' ' in command[0]: 
            command = command[0].split()
        
        print(command[0])
        
        if command[0] not in command_list: 
            print('invalid command, try help')
        else:
            if(command[0] == 'quit'):
                cont = False 
            else: 
                call_function(command)



main() 
