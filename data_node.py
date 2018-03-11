import os
import rpyc
import pickle
from reply import Reply
import time
import threading

#namenode information
NN_IP = '35.167.176.87'
PORT = '5000'
MY_IP = ''

class DataNodeService(rpyc.Service):
    class exposed_BlockStore:
        def __init__(self, file_name='persistent.dat'):
            self.block_id = set()
            self.name_map = file_name
            self.load_node()
            #self.block_report_timer()

        #load data node if previously started
        def load_node(self):
            print('Loading previous storage')
            try:
                with open(self.name_map, 'rb') as f:
                    while True:
                        try:
                            self.block_id.add(pickle.load(f))
                        except EOFError:
                            break
            except:
                print('Could not load persistent data, creating new')
                self.block_id = set()

        #write block to block report
        def save_block(self, id):
            print('saving ', id)
            with open(self.name_map, 'a+b') as f:
                pickle.dump(id, f)
            f.close()
            self.block_id.add(id)
            print('block report is', self.block_id)

        def block_report_timer(self):
            threading.Timer(60.0, self.block_report_timer).start()
            self.block_report()

        #creates a list of block_ids
        def block_report(self):
            blocks = []
            print("sending block report!!")
            with open(self.name_map, 'rb') as f:
                while 1:
                    try:
                        blocks.append(pickle.load(f))
                    except EOFError:
                        break
            f.close()
            print('printing block report')
            print(blocks)
            c = rpyc.connect(NN_IP, PORT)
            cmds = c.root.receive_block_report(MY_IP, blocks).split(',')
            self.parse_commands(cmds)

            return blocks

        def parse_commands(self, cmds):
            while len(cmds) > 0:
                cmd = cmds.pop(0)
                if cmd == "delete":
                    path = cmd.pop(0)
                    self.exposed_delete_block(path)
                elif cmd == "forward":
                    path = cmd.pop(0)
                    dest = cmd.pop(0)
                    c = rpyc.connect(dest, 5000)
                    next_node = c.root.BlockStore()
                    reply = Reply.Load(next_node.put_block(path, self.exposed_get_block(path), [dest]))
                    print(reply.status)
                else:
                    self.exposed_delete_all()

        #save block to storage from client request
        def exposed_put_block(self, file_name, data, replica_node_ids):
            print('new file name', file_name)
            if file_name in self.block_id:
                return Reply.error('File name already exists')
            else:
                try:
                    with open(file_name, 'wb') as f:
                        f.write(data)
                except:
                    self.block_id.remove(id)
                    return Reply.error('Error saving block')

                self.save_block(file_name)

                # send out replicas
                replica_node_ids.pop(0)
                if len(replica_node_ids) > 0:
                    done = 1
                    tries = 0
                    print("Sending replica to ", replica_node_ids[0])
                    while done == 1:
                        c = rpyc.connect(replica_node_ids[0], 5000)
                        next_node = c.root.BlockStore()
                        reply = Reply.Load(next_node.put_block(file_name, data, replica_node_ids))
                        print(reply.status)
                        if reply.status == 0:
                            print("replica sent!")
                            done = 0
                        else:
                            print("node busy trying again")
                            # wait 5 seconds and try again
                            time.sleep(5)
                            tries += 1
                            if tries > 4:
                                # after 4 tries give up
                                print("could not send block replica")
                                break
                return Reply.reply()

        #retrieve a block, given a block name
        def exposed_get_block(self, file_name):
            if file_name in self.block_id:
                read_file = open(file_name, 'rb')
                data = read_file.read()
                read_file.close()
                return Reply.reply(data)
            else:
                return Reply.error('File not found')

        #delete a block, given a block name
        def exposed_delete_block(self, id):
            if id in self.block_id:
                self.block_id.remove(id)
                os.remove(id)
                return Reply.reply()
            else:
                return Reply.error('Block not found')

        #deletes all blocks in storage
        def delete_all(self):
            blocks = []
            print("sending block report!!")
            with open(self.name_map, 'rb') as f:
                while 1:
                    try:
                        blocks.append(pickle.load(f))
                    except EOFError:
                        break
            f.close()
            for b in blocks:
                self.block_id.remove(b)
                os.remove(b)

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    bs = DataNodeService.exposed_BlockStore()
    bs.block_report_timer()
    t = ThreadedServer(DataNodeService, port=5000)
    t.start()