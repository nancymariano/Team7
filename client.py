#client module for SUFS

import socket
#import click
import re
import rpyc 
import pickle 


from boto.s3.connection import S3Connection
from boto.s3.key import Key
from reply import Reply 
from io import BytesIO 
import boto
import os 

file_pattern = re.compile('^\\(.+\\)*(.+)\.(.+)$')
block_size = 128*1024
#data_node_IP = '' 
name_node_IP = '34.210.149.98'
#node_IPs = ['ip1','ip2']


#this is a built in function of click that automatically creates a help menu
def help(ctx, topic,**kw):
    '''Show this message and exit'''
    click.echo(cli.commands[topic].get_help(ctx))    

#create will allow the user to create a file 
#it will then tell the user if it was not successful  
def make_file(file_path, to_path):
    #user will need to make their file publically accessible for the scope of assignment 
    #s3_client = boto.connect_s3()
    #m = file_pattern.match(to_path) 
    #file_name= m.group() 
    #s3_client.download_file('s3_obj', 'file_name', 'client_file')

    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    #name_reply = Reply.Load(name_node.make_file(os.path.getsize(to_path)), to_path))
    file_size = os.path.getsize(file_path)
    f = open(file_path, 'rb')
    name_reply = name_node.make_file(file_size, to_path)

    print(file_size) 
    print(name_reply)

    #block_locations = name_reply.result
    block_locations = name_reply

    send_blocks = [] 
    #j = 1 
    k = 0
 
    with open(file_path, 'rb') as input: 
        bytes = input.read(block_size) 
        while len(bytes) > 0:
            bytes = input.read(block_size)
            node_IPs = block_locations[k+1].split('{')[1]
            node_IPs = node_IPs.split('}')[0]
            node_IPs = node_IPs.split(',')
            send_blocks.append([block_locations[k], bytes, node_IPs])
            #j+=2 
            k+=2

    for block in send_blocks: 
        data_node_IP = block[2][0]
        print(data_node_IP)
        data_conn = rpyc.connect(data_node_IP, 5000, config = {'allow_public_attrs': True})
        data_node = data_conn.root 

        print(block[0], block[1], block[2])
        data_reply = Reply.Load(data_node.put_block(block[0], block[1], block[2]))
        print(data_reply.status)
        print('result: ', repr(data_reply))



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
def read_file(path):
    m = file_pattern.match(to_path) 
    file_name= m.group() 

    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root

    block_locations = []
    name_reply = name_node.read_file(path, block_locations)

    temp = block_locations.split(',')
    receive_blocks = [] 
    i = 0 
    for i in range(0, len(block_locations), 2): 
        receive_blocks[i] = [block_locations[i]]
        node_IPs = block_locations[i+1].split('{')[1]
        node_IPs = node_IPs.split('}')[0]
        node_IPs = node_IPs.split(',')
        receive_blocks[i][0].append(node_IPs)
    
    with open(file_name, 'a') as output: 
        for block in receive_blocks:
            data_conn = rpyc.connect(block[1][0], 5000, config = {'allow_public_attrs': True})
            data_node = data_conn.root
            data_reply = Reply.Load(data_node.get_block(block))
            if not data_reply.error:
                client_file.append(data_reply.result)

    return client_file
    
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
        if len(command) != 3: 
            print('error: wrong number args')
        else: 
            make_file(command[1], command[2]) 
    if(command[0] == 'rm'):
        if len(command) != 2: 
            print('error: wrong number args')
        else: 
            delete_file(command[1]) 
    if(command[0] == 'ls'):
        if len(command) != 2: 
            print('error: wrong number args')
        else: 
            list_dir(command[1]) 
    if(command[0] == 'download'):
        if len(command) != 2: 
            print('error: wrong number args')
        else: 
            read_file(command[1])
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
