#client module for SUFS

import socket
import click
import re
import rpyc 
import pickle 


from boto.s3.connection import S3Connection
from boto.s3.key import Key
from reply import Reply 
from io import BytesIO 
import boto
import os 

ACCESS_KEY = ''
SECRET_KEY = ''
bucket_name = ''
conn = S3Connection(ACCESS_KEY, SECRET_KEY)
bucket = conn.get_bucket(bucket_name)

file_pattern = re.compile('^\\(.+\\)*(.+)\.(.+)$')
block_size = 128 
#data_node_IP = '' 
name_node_IP = '35.167.176.87'
node_IPs = ['ip1','ip2']

@click.group()
def cli():
    pass

@cli.command()
@click.argument('topic', default=None, required=False, nargs=1)
@click.pass_context

#this is a built in function of click that automatically creates a help menu
def help(ctx, topic,**kw):
    '''Show this message and exit'''
    click.echo(cli.commands[topic].get_help(ctx))    

#create will allow the user to create a file 
#it will then tell the user if it was not successful  
@cli.command('create')
@click.argument('s3_obj', 'to_path', default=None, required=False, nargs=2)
@click.pass_context
def make_file(s3_obj, to_path):
    #user will need to make their file publically accessible for the scope of assignment 
    s3_client = boto.connect_s3()
    m = file_pattern.match(to_path) 
    file_name= m.group() 
    s3_client.download_file('s3_obj', 'file_name', 'client_file')

    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root.BlockStore()

    name_reply = Reply.Load(name_node.make_file(os.stat(client_file), to_path))
    
    block_locations = name_reply.result

    send_blocks = [] 
    j = 1 
    k = 0
    with open(file_name, 'rb') as input: 
        bytes = input.read() 
        for i in range(0,len(bytes), block_size): 
            send_blocks.append([block_locations[k], bytes[i: i+block_size], block_locations[j]])
            j+=2 
            k+=2

    for block in send_blocks: 
        data_node_IP = block[1][0]
        data_conn = rpyc.connect(data_node_IP, 5000, config = {'allow_public_attrs': True})
        data_node = data_conn.root.BlockStore()     
        data_reply = Reply.Load(data_node.put_block(block[0], block[1], block[2]))
        print(data_reply.status)



#delete will allow the user to delete a file given a full file path 
@cli.command('delete')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def delete_file(path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root.BlockStore()

    name_reply = Reply.Load(name_node.delete_path(path))
    
    delete_result = name_reply.result
    if (delete_result.status != 1): 
        print("error deleting file")


#delete will allow the user to read a file given a full file path 
#it will ask NameNode to delete
#it will then tell the user whether or not the delete was successful
#if the file does not exist, it will tell the user
@cli.command('read')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def read_file(path):
    m = file_pattern.match(to_path) 
    file_name= m.group() 

    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root.BlockStore()

    block_locations = []
    name_reply = Reply.Load(name_node.read_file(path, block_locations))

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
            data_node = data_conn.root.BlockStore()
            data_reply = Reply.Load(data_node.get_block(block))
             if not data_reply.error:
                 client_file.append(data_reply.result)

    return client_file
    
#mkdir will allow the user to make a directory in SUFS
#it will ask the NameNode to create the path 
#the user will be notified if the path could not be created
@cli.command('mkdir')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def make_dir(file_name):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root.BlockStore()

    name_reply = Reply.Load(name_node.create_directory(path))

    if not name_reply.result:
        print("Could not make directory") 
        


#rmdir will allow the user to remove a directory in SUFS if it is empty 
#it will ask the NameNode to delete the path 
#the user will be notified if the delete was not successful 
@cli.command('rmdir')
@click.argument('file_name', default=None, required=False, nargs=1)
@click.pass_context
def delete_dir(file_name):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root.BlockStore()

    name_reply = Reply.Load(name_node.delete_path(path))

    if not name_reply.result:
        print("Could not remove directory")

#lsdir will allow the user to list the contents of a directory if it exists 
#it will ask the NameNode for the names of the contents in the directory 
#the user will see a listing of the contents if the directory exists
@cli.command('lsdir')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def list_dir(path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root.BlockStore()

    content_names = [] 
    name_reply = Reply.Load(name_node.list_directory(path, content_names))

    if not name_reply.result: 
        for name in content_names: 
            print(name)
    else: 
        print("Could not list directory contents")

#lsdata will allow the user to list the data blocks of a file given a file path  
#it will ask the NameNode for the data blocks which store the contents of the file  
#the user will see a listing of the blocks if the file exists     
@cli.command('lsdata')
@click.argument('file_path', default=None, required=False, nargs=1)
@click.pass_context
def list_data_nodes(file_path):
    name_conn = rpyc.connect(name_node_IP, 5000, config = {'allow_public_attrs': True})
    name_node = name_conn.root.BlockStore()

    block_locations = []
    name_reply = Reply.Load(name_node.read_file(path, block_locations))

    if not name_reply.error:
        for block in block_locations: 
            print(block[1])
    else: 
        print("Data blocks could not be listed")
