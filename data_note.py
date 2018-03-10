import os
import rpyc
import pickle
from reply import Reply
import time
import boto

#namenode information
NN_IP = ''
PORT = ''
MY_IP = ''

#ec2 information
ACCESS_KEY = ''
SECRET_KEY = ''
AMI_ID = ''
SECUR_GROUP = ''


class DataNodeService(rpyc.Service):
    class exposed_BlockStore:
        def __init__(self, file_name='persistent.dat'):
            self.block_id = set()
            self.name_map = file_name
            self.load_node()
            self.connect_to_aws()

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

        def connect_to_aws(self):
            self.conn = boto.ec2.connect_to_region('us-west-2',
                                              aws_access_key_id=ACCESS_KEY,
                                              aws_secret_acces_key=SECRET_KEY)

        #write block to block report
        def save_block(self, id):
            print('saving ', id)
            with open(self.name_map, 'a+b') as f:
                pickle.dump(id, f)
            f.close()
            self.block_id.add(id)
            print('block report is', self.block_id)

        #creates a list of block_ids
        def block_report(self):
            blocks = []
            with open(self.name_map, 'rb') as f:
                while 1:
                    try:
                        blocks.append(pickle.load(f))
                    except EOFError:
                        break
            f.close()
            print('printing block report')
            print(blocks)
            return blocks

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

        # Precondition:
        # Postcondition: block report is sent
        # Function: sends updates on what is being stored on the DataNode
        def send_block_report(self, ip, path):
            dir = os.listdir(path)
            block_list = []
            for path, dirs, files in os.walk(path):
                for filename in files:
                    block_list.append(filename)
            c = rpyc.connect(NN_IP, PORT)
            cmds = c.root.receive_block_report(MY_IP, block_list)

        #start a new data node by creating a block device mapping
        #and then running an instance
        def exposed_replicate_node(self):
            block_dev = boto.ec2.blockdevicemapping.EBSBlockDeviceType()
            block_dev.size = 8 #size in GB
            bdm = boto.ec2.blockdevicemapping.BlockDeviceMapping()
            bdm['/dev/sda1'] = block_dev

            new_inst = self.conn.run_instances(image_id=AMI_ID,
                                         key_name='name',
                                         instance_type='t2.micro',
                                         security_groups=SECUR_GROUP)
            #and then call this file in that instance... yeah
            pass

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(DataNodeService, port=5000)
    t.start()
    #sendBlockReport(path)