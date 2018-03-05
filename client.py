import rpyc

block_size = 128
data_node_IP = '54.202.89.188'

conn = rpyc.connect(data_node_IP, 5000, config={'allow_public_attrs': True})
print("Connecting with server...")

remoteFile = conn.root.Block()

#testing sending a file over
#opens and closes remote file
remoteFile.open('testFile.txt', 'wb')
localFile = open('testFile.txt', 'rb')

chunk = localFile.read(block_size*block_size)
while chunk:
    remoteFile.write(chunk)
    chunk = localFile.read(block_size*block_size)

remoteFile.close()
localFile.close()
print('Server: finished')

print("Closing connection with server.")
conn.close()