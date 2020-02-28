from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto

ACCESS_KEY = ''
SECRET_KEY = ''
bucket_name = ''

conn = S3Connection(ACCESS_KEY, SECRET_KEY)
bucket = conn.get_bucket(bucket_name)

