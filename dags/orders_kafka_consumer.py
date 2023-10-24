from kafka import KafkaConsumer
import time
import logging
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement

import json

#load_balancing_policy=RoundRobinPolicy()
#protocol_version=4

class CassandraConnector:
    def __init__(self, contact_points, keyspace):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect(keyspace)
    
    def insert_data(self, data):
        insert_statement =  self.session.prepare(""" INSERT INTO all_orders_table (event, messageid, userid, lineitems, orderid, timestamp)
                                                     VALUES (?, ?, ?, ?, ?, ?)""")
        
        self.session.execute(insert_statement, data)


    def shutdown(self):
        self.cluster.shutdown()

def fetch_and_insert_data_orders():

    consumer = KafkaConsumer('order_topic', 
                             bootstrap_servers=['192.168.1.105:9092'],
                             auto_offset_reset='earliest'
                             #group_id='orders_group',
                             #value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                            )
    
    cassandra_connector = CassandraConnector(['cassandra'],keyspace='orders_data_keyspace')

    duration_secs = 60
    start_time = time.time()
    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time >= duration_secs:
            break
        try:
            for message in consumer:
                message_value = json.loads(message.value)
            
                order_data = (
                message_value['event'],
                message_value['messageid'],
                message_value['userid'],
                message_value['lineitems'],
                message_value['orderid'],
                message_value['timestamp']
            )
                cassandra_connector.insert_data(data=order_data)
        
        except Exception as e:
                print(f"An error occurred: {str(e)}")

        finally:
            cassandra_connector.shutdown()
            consumer.close()


if __name__ == '__main__': 
    fetch_and_insert_data_orders()



    

    