import sys
import os
import rpyc
import pickle
import socket
from reply import Reply
import threading

from rpyc.utils.server import ThreadedServer

# Namenode information
NAMENODE_IP_ADDR = '34.210.149.98'
NAMENODE_PORT = 5000
DATANODE_PORT = 5000
DATANODE_IP_ADDR = '34.217.74.84'
BLOCK_REPORT_FREQ = 15.0


class DataStore:
    """
    Responsible for reading/writing/deleting files
    Sends block report to namenode periodically
    """

    instance = None  # Singleton pattern - only one instance of this class allowed

    def __init__(self, data_set_file_name='persistent.dat'):
        if DataStore.instance is not None:
            raise Exception('There should only be one instance of DataStore, use static get_instance() method')

        self.stored_blocks = set()
        self.data_set_fname = data_set_file_name

    @classmethod
    def get_instance(cls):
        """
        Returns a single global shared instance of this class. Calls constructor on the first call.
        :return: Single global shared instance of DataStore
        """
        if cls.instance is None:
            cls.instance = DataStore()
        return cls.instance

    # Load data node if previously started
    def load_file_set(self):
        print('Loading previous storage')
        try:
            with open(self.data_set_fname, 'rb+') as f:
                pickle_data = f.read()
                file_set = pickle.loads(pickle_data)
                self.stored_blocks = file_set
        except:
            print('Could not load persistent data, creating new')

    # Saves block names to file on disk
    def save_blocks_set(self):
        print('Saving the set of blocks to disk..')
        with open(self.data_set_fname, 'wb+') as f:
            f.write(pickle.dumps(self.stored_blocks))

    # Sends timed block reports to namenode
    def block_report_timer(self):
        threading.Timer(BLOCK_REPORT_FREQ, self.block_report_timer).start()
        self.block_report()

    # Creates a list of blocks
    def block_report(self, send_to_namenode=True):
        blocks = list(self.stored_blocks)

        print('Printing block report: ', blocks)

        if send_to_namenode:
            print("Sending block report")
            try:
                c = rpyc.connect(NAMENODE_IP_ADDR, NAMENODE_PORT)
                cmds = c.root.receive_block_report(DATANODE_IP_ADDR, blocks).split(',')
                self.parse_commands(cmds)
            except Exception as e:
                print('Unable to send block report to namenode')
                print(e)

        return blocks

    # Parses commands from namenode in response to block report
    def parse_commands(self, cmds):
        while len(cmds) > 0:
            print('Commands from namenode: ', cmds)
            cmd = cmds.pop(0)
            if cmd == "delete":
                path = cmds.pop(0)
                if path == '*':
                    self.delete_all()
                else:
                    self.delete_block(path)
            elif cmd == "forward":
                path = cmds.pop(0)
                dest = cmds.pop(0)
                c = rpyc.connect(dest, DATANODE_PORT)
                next_node = c.root
                reply = Reply.Load(next_node.put_block(path, self.get_block(path), [dest]))
                print('reply:', reply)
            else:
                pass

    # Delete a block in storage, and in list of blocks
    def delete_block(self, id):
        if id not in self.stored_blocks:
            raise Exception('Block not found')

        os.remove(id)

        self.stored_blocks.remove(id)
        self.save_blocks_set()

    # Retrieve a block, given a block name
    def get_block(self, file_name):
        if file_name not in self.stored_blocks:
            print('could not find file')
            raise Exception('File not found')
            return Reply.error('file not found')

        with open(file_name, 'rb+') as f:
            return f.read()

    # Save block to storage from client request
    def put_block(self, file_name, data, forward_to_nodes):
        print('new file name', file_name)
        if file_name in self.stored_blocks:
            raise Exception('File name already exists')
        try:
            with open(file_name, 'wb+') as f:
                f.write(data)
        except:
            print('could not open data')
            raise Exception('Could not open data')

        print('File written!')

        # save it to the set of blocks and persist to disk
        self.stored_blocks.add(file_name)
        self.save_blocks_set()

        forward_to_nodes = forward_to_nodes[1:]
        # forward block to rest of nodes
        if len(forward_to_nodes) > 0:
            print("Forwarding replicas to ", forward_to_nodes)
            next_node = forward_to_nodes[0]
            conn = rpyc.connect(next_node, DATANODE_PORT)
            next_node = conn.root

            put_ret = next_node.put_block(file_name, data, forward_to_nodes)
            reply = Reply.Load(put_ret)
            print('Next node replied with: ', str(reply))
            if reply.is_ok():
                print("replica sent!")
            else:
                print('Unable to send: ', reply.err)
                # raise Exception(reply.err)
        return Reply.reply()

    # Deletes all blocks in storage
    def delete_all(self):
        print("deleting all blocks")
        for b in self.stored_blocks:
            self.stored_blocks.remove(b)
            os.remove(b)


class DataNodeService(rpyc.Service):
    # Gets an instance of DataStore on connect
    def on_connect(self):
        super().on_connect()
        self._data_store = DataStore.get_instance()
        print('Someone connected to me')

    def on_disconnect(self):
        super().on_disconnect()
        print('Someone disconnected from me')

    # Save block to storage from client request
    def exposed_put_block(self, file_name, data, forward_node_locations):
        try:
            self._data_store.put_block(file_name, data, forward_node_locations)
            return Reply.reply()
        except:
            return Reply.error('Could not put block')

    # Retrieve a block, given a block name
    def exposed_get_block(self, file_name):
        try:
            data = self._data_store.get_block(file_name)
            return Reply.reply(data)
        except:
            return Reply.error('Could not get block')

    # Delete a block, given a block name
    def exposed_delete_block(self, id):
        try:
            self._data_store.delete_block(id)
            return Reply.reply()
        except:
            return Reply.error('Could not delete block')


def run_tests():
    store = DataStore.get_instance()
    store.delete_all()
    store.put_block('file1', b'test data foo bar', ['localhost'])
    store.block_report(send_to_namenode=False)
    store.put_block('file2', b'file 2 data', ['localhost'])
    store.put_block('file3', b'file 3 data data data', ['localhost'])
    store.block_report(send_to_namenode=False)

    print('getting some data back..')
    file1_get_data = store.get_block('file1')
    print('got: ', file1_get_data)

    print('deleting file1..')
    store.delete_block('file1')
    store.block_report(send_to_namenode=False)

    try:
        file1_get_data = store.get_block('file1')
        print('SHOULD NOT FIND FILE AFTER DELETED')
    except:
        print('file1 not found')

    print('deleted block from this namenode, attempting to get it from replica..')
    node2_conn = rpyc.connect('localhost', DATANODE_PORT)
    node2 = node2_conn.root
    node2_reply = Reply.Load(node2.get_block('file1'))
    if node2_reply.is_ok():
        node2_data = node2_reply.result
        print('got data from replica: ', node2_data)
    else:
        print('could not get data from replica', node2_reply.err)

    print('delete file2 from replica..')
    node2_reply = Reply.Load(node2.delete_block('file2'))
    print('result: ', repr(node2_reply))

    print('attempting to get the deleted file from replica, should fail..')
    node2_reply = Reply.Load(node2.get_block('file2'))
    if node2_reply.is_ok():
        print('Error: should have failed getting a deleted file')
    else:
        print('Success: cant get deleted: ', node2_reply.err)


if __name__ == '__main__':

    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        run_tests()
        # When in test mode, do not start the server
        sys.exit(0)

    server_listen_port = DATANODE_PORT

    # check if user supplied port number to listen to
    if len(sys.argv) > 1:
        server_listen_port = int(sys.argv[1])

    # Initialize a timer to periodically send block reports to namenode
    DataStore.get_instance().block_report_timer()

    print('Starting rpc server..')
    server = ThreadedServer(DataNodeService, port=5000)
    server.start()

