#client module for SUFS

import socket
import click

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
#it will ask the NameNode for where to place the file 
#it will then partition the file into 128MB blocks
#it will then send each file to the DataNode for storage
#it will then tell the user if it was not successful  
@cli.command('create')
@click.argument('s3_obj', 'to_path', default=None, required=False, nargs=2)
@click.pass_context
def make_file(s3_obj):

#delete will allow the user to delete a file given a full file path 
#it will ask NameNode to delete
#it will then tell the user if the delete was unsuccessful
#if the file does not exist, it will tell the user
@cli.command('delete')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def delete_file(path):


#delete will allow the user to read a file given a full file path 
#it will ask NameNode to delete
#it will then tell the user whether or not the delete was successful
#if the file does not exist, it will tell the user
@cli.command('read')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def read_file(path):

#mkdir will allow the user to make a directory in SUFS
#it will ask the NameNode to create the path 
#the user will be notified if the path could not be created
@cli.command('mkdir')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def make_dir(file_name):

#rmdir will allow the user to remove a directory in SUFS if it is empty 
#it will ask the NameNode to delete the path 
#the user will be notified if the delete was not successful 
@cli.command('rmdir')
@click.argument('file_name', default=None, required=False, nargs=1)
@click.pass_context
def delete_dir(file_name):

#lsdir will allow the user to list the contents of a directory if it exists 
#it will ask the NameNode for the names of the contents in the directory 
#the user will see a listing of the contents if the directory exists
@cli.command('lsdir')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def list_dir(path):

#lsdata will allow the user to list the data blocks of a file given a file path  
#it will ask the NameNode for the data blocks which store the contents of the file  
#the user will see a listing of the blocks if the file exists     
@cli.command('lsdata')
@click.argument('file_path', default=None, required=False, nargs=1)
@click.pass_context
def list_data_nodes(file_path):

