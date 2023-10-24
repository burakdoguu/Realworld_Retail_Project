from kafka import KafkaProducer
import time
import json
import pandas as pd
import traceback
import logging



def view_product():

#    data = "/opt/airflow/resources/product-views.json"
    data = "C:\\Users\\QP\\Desktop\\new_project\\resources\\product-views.json"
    df = pd.read_json(data,lines=True)
    review_df = df[['event', 'messageid', 'userid', 'properties', 'context']]
    review_dict = review_df.to_dict(orient="records")
    
    try:
        for item in review_dict:
            event_name = str(item['event'])
            message_id = str(item['messageid'])            
            user_id = str(item['userid'])
            properties = str(item['properties'])
            context = str(item['context'])
            timestamp = str(int(time.time()))
            producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'),acks='all',retries=1)
            producer.send('view_product_topic', {"event": event_name, "messageid": message_id, "userid": user_id, "properties": properties,
                    "context": context, "timestamp": timestamp})
            producer.flush()
            time.sleep(5)
    except Exception as e:
        logging.error(traceback.format_exc())


if __name__ =="__main__":
    view_product()

