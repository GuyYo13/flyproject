from kafka import KafkaConsumer
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime
import json

# Kafka topic and broker details
topic = 'lv1'
brokers = ['cnt7-naya-cdh63:9092']

# HDFS output file path
hdfs_path = 'hdfs://cnt7-naya-cdh63:8020/user/hdfs/flyproject/'

# Create an HDFS filesystem object with credentials
hdfs = pa.hdfs.connect(host='cnt7-naya-cdh63', port=8020, user='hdfs')

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    client_id='consumerhdfsnew',
    group_id='2',
    bootstrap_servers=brokers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)

now = datetime.now()
filecounter=0
filename=now.strftime("%Y-%m-%d %H:%M:%S")
filename=str(filename).replace(" ", "_").replace(":", "").replace("-", "_").replace(".", "_")+ '.json'

for message in consumer:
    
    json_data = json.loads(message.value)
    json_string = json.dumps(json_data)
    
    hdfs_output_path=hdfs_path+filename
    with hdfs.open(hdfs_output_path, 'wb') as file:
        file.write(json_string.encode('utf-8'))

    file_info = hdfs.info(hdfs_output_path)
    file_size = file_info['size']
    if file_size>100000:
       if filecounter==10:
          filecounter=0
       filecounter+=1
       now = datetime.now()
       filename=now.strftime("%Y-%m-%d %H:%M:%S")
       filename=str(filename).replace(" ", "_").replace(":", "").replace("-", "_").replace(".", "_")+'_'+ str(filecounter) + '.json'
       print ('the new file name :' ,hdfs_output_path)
        
    
#hdfs.close()