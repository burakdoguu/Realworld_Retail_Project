from kafka import KafkaProducer
import time
import json
import pandas as pd
import traceback
import logging


def orders_data():
    data = "C:\\Users\\QP\\Desktop\\new_project\\resources\\orders.json"
    #data = "/opt/airflow/resources/orders.json"
    df = pd.read_json(data,lines=True)
    review_df = df[['event', 'messageid', 'userid', 'lineitems', 'orderid']]
    review_dict = review_df.to_dict(orient="records")

    try:
        for item in review_dict:
            user_id = str(item['userid'])
            line_items = str(item['lineitems'])
            event_name = str(item['event'])
            message_id = str(item['messageid'])
            order_id = int(item['orderid'])
            timestamp = str(int(time.time()))
            producer = KafkaProducer(bootstrap_servers=['{your_ip}:9092'],value_serializer=lambda m: json.dumps(m).encode('utf-8'),acks='all',retries=1)
            producer.send('order_topic', {"event": event_name, "messageid": message_id, "userid": user_id, "lineitems": line_items,
                    "orderid": order_id, "timestamp": timestamp})
            producer.flush()
            time.sleep(60)
    except Exception as e:
        logging.error(traceback.format_exc())

if __name__ =="__main__":
    orders_data()


