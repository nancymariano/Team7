#client module for SUFS

import socket
import click

@click.group()
def cli():
    pass

@cli.command()
@click.argument('topic', default=None, required=False, nargs=1)
@click.pass_context
def help(ctx, topic,**kw):
    '''Show this message and exit'''
    click.echo(cli.commands[topic].get_help(ctx))

@cli.command('create')
@click.argument('s3_obj', 'to_path', default=None, required=False, nargs=2)
@click.pass_context
def make_file(s3_obj):


@cli.command('delete')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def delete_file(path):

@cli.command('read')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def read_file(path):


@cli.command('mkdir')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def make_dir(file_name):

@cli.command('rmdir')
@click.argument('file_name', default=None, required=False, nargs=1)
@click.pass_context
def delete_dir(file_name):

@cli.command('lsdir')
@click.argument('path', default=None, required=False, nargs=1)
@click.pass_context
def list_dir(path):

@cli.command('lsdata')
@click.argument('file_path', default=None, required=False, nargs=1)
@click.pass_context
def list_data_nodes(file_path):

