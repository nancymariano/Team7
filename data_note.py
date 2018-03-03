import os
import rpyc
import boto

# Precondition: file is not duplicated
# Postcondition: file is stored 
# Function: accepts a data tranasfer from a client
def acceptWriteFromClient(c, b, path):
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
def sendBlockReport(path):
    dir = os.listdir(path)
    block_list = []
    for path, dirs, files in os.walk(path):
        for filename in files:
            block_list.append(filename)
    return(block_list)

# Precondition: request for a block is received from client
# Postcondition: block is sent to the client
# Function: a
def sendBlockToClient(path): #if client sends a request
    return

# Precondition: block is received from client
# Postcondition: block is stored, confirmation is sent to client
# Function:
def storeBlock(fileobj):
    replicateBlock(fileobj)
    return

# Precondition: request to delete a block is recvd from client
# Postcondition: block is removed from storage, and confirmation is sent
# Function:
def deleteBlock(path):
    if os.path.isfile(path):
        os.remove(path)
    else:
        return("File doesn't exist")

# Precondition: a block is received from another DataNode
# Postcondition: a block is forwarded to the next DataNode in the path
# Function:
def replicateBlock(path):
    client = rpyc.connect('localhost', 18861, config={'allow_public_attrs': True})
    return

if __name__ == '__main__':
    #t = ThreadedServer(S3Connect, port=18861)
    #t.start()
    sendBlockReport(path)