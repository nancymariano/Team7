# Precondition: file is not duplicated 
# Postcondition: file is stored 
# Function: accepts a data tranasfer from a client
def acceptWriteFromClient(c, b, path):


# Precondition: 
# Postcondition: block report is sent 
# Function: sends updates on what is being stored on the DataNode
def sendBlockReport(paths):


# Precondition: request for a block is received from client
# Postcondition: block is sent to the client
# Function: a
def sendBlockToClient(path): #if client sends a request


# Precondition: block is received from client
# Postcondition: block is stored, confirmation is sent to client
# Function:
def storeBlock():


# Precondition: request to delete a block is recvd from client
# Postcondition: block is removed from storage, and confirmation is sent
# Function:
def deleteBlock(path):


# Precondition: a block is received from another DataNode
# Postcondition: a block is forwarded to the next DataNode in the path
# Function:
def replicateBlock(path):