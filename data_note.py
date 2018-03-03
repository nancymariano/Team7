import os
import rpyc
import boto

# Class for rpyc service

class DataNodeService(rpyc.Service):
    NN_IP = ""
    MY_IP = ""
    PORT = 5000

    # Precondition: file is not duplicated
    # Postcondition: file is stored
    # Function: accepts a data tranasfer from a client
    def exposed_acceptWriteFromClient(self, c, b, path):
        if os.path.isfile(b):
            return "File name already exists"
        else:
            lines = b.read()
            new_path = path + b
            new_file = open(new_path, 'w')
            new_file.write(lines)
            new_file.close()

    # Precondition:
    # Postcondition: block report is sent
    # Function: sends updates on what is being stored on the DataNode
    def sendBlockReport(self, ip, path):
        dir = os.listdir(path)
        block_list = []
        for path, dirs, files in os.walk(path):
            for filename in files:
                block_list.append(filename)
        c = rpyc.connect(NN_IP, PORT)
        cmds = c.root.receive_block_report(MY_IP, block_list)

    # Precondition: request for a block is received from client
    # Postcondition: block is sent to the client
    # Function: a
    def sendBlockToClient(self, path): #if client sends a request
        return

    # Precondition: block is received from client
    # Postcondition: block is stored, confirmation is sent to client
    # Function:
    def storeBlock(self, fileobj):
        replicateBlock(fileobj)
        return

    # Precondition: request to delete a block is recvd from client
    # Postcondition: block is removed from storage, and confirmation is sent
    # Function:
    def exposed_deleteBlock(self, path):
        if os.path.isfile(path):
            os.remove(path)
        else:
            return("File doesn't exist")

    # Precondition: a block is received from another DataNode
    # Postcondition: a block is forwarded to the next DataNode in the path
    # Function:
    def replicateBlock(self, path):
        client = rpyc.connect('localhost', 5000, config={'allow_public_attrs': True})
        return


    def exposed_test(self, message):
        print("Received Message: " + message)
        return "got ur message thx"

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(DataNodeService, port=5000)
    t.start()
    sendBlockReport(path)