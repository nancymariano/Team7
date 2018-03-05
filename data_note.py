import os
import rpyc

class DataNode(rpyc.Service):
    class exposed_Block():
        def exposed_open(self, filename, mode = 'r'):
            print("Received call to store file: ", filename)
            self.__filehandle = open(filename, mode)
            self.__filename = filename

        def exposed_write(self, bytes):
            return self.__filehandle.write(bytes)

        def exposed_close(self):
            print("Closing file: ", self.__filename)
            return self.__filehandle.close()


    # Precondition: file is not duplicated
    # Postcondition: file is stored
    # Function: accepts a data tranasfer from a client
    def exposed_acceptWriteFromClient(self, b, path, destinations):
        if os.path.isfile(b):
            return "File name already exists"
        else:
            self.exposed_replicateBlock()


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
    def exposed_sendBlockToClient(self, path): #if client sends a request
        return

    # Precondition: block is received from client
    # Postcondition: block is stored, confirmation is sent to client
    # Function:
    def storeBlock(self, file, path):
        lines = file.read()
        new_path = path + file
        new_file = open(new_path, 'w')
        new_file.write(lines)
        new_file.close()
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
    def exposed_replicateBlock(self, file, path, destinations):
        self.storeBlock(file, path)
        destinations = destinations[1:]
        if len(destinations) > 0:
            c = rpyc.connect(destinations[0], 5000)
            c.root.replicateBlock(file, path, destinations)
        return


    def exposed_test(self, message):
        print("Received Message: " + message)
        return "got ur message thx"

    #testing client info stuff... not useful
    def exposed_get_conn_info(self):
        s_ConnectionID = self._conn._config['connid']
        s_Credentials = self._conn._config['credentials']
        s_HostAdress, s_HostPort = self._conn._config['endpoints'][0]
        s_ClientAdress, s_ClientPort = self._conn._config['endpoints'][1]
        connList = [s_ConnectionID, s_Credentials, s_HostAdress, s_HostPort, s_ClientAdress, s_ClientPort]
        return connList

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(DataNode, port=5000)
    t.start()
    #sendBlockReport(path)