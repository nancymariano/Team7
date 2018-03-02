from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto

ACCESS_KEY = ''
SECRET_KEY = ''
bucket_name = ''

conn = S3Connection(ACCESS_KEY, SECRET_KEY)
bucket = conn.get_bucket(bucket_name)

#store new object
# k = Key(bucket)
# k.key = 'test'
# k.set_contents_from_string('Test of S3')

#retrieve object
# k2 = Key(bucket)
# k2.key = 'test'
# print(k2.get_contents_as_string())

#print all buckets in dir
# all_buckets = conn.get_all_buckets()
# print("Current buckets:", len(all_buckets))
# for bucket in all_buckets:
#     print(bucket.name)

#print bucket's contents
# for item in bucket.list():
#     print(item.name, "\t", item.size, "\t", item.last_modified)

#delete item from bucket
# print("Deleting item...")
# bucket.delete_key('test')
# print("Deletion complete")

