from confluent_kafka import Consumer, KafkaException, KafkaError
from datetime import datetime
import warnings
import pymysql
import time
import json
import sys
import os


# Database configuration / The place where we will finally store the data coming from the Kafka queue
 
database_config = {"hostname" : "localhost", 
                   "username" : "root", 
                   "password" : "", 
                   "database" : "ingestion_database"}

# Kafka configuration / Self-explained!

kafka_config = {'bootstrap.servers'     : 'rocket-01.srvs.cloudkafka.com:9094,rocket-02.srvs.cloudkafka.com:9094,rocket-03.srvs.cloudkafka.com:9094',
                'group.id'              : "cdupjrrn-consumer",
                'session.timeout.ms'    : 6000,
                'default.topic.config'  : {'auto.offset.reset': 'smallest'},
                'security.protocol'     : 'SASL_SSL',
	            'sasl.mechanisms'       : 'SCRAM-SHA-256',
                'sasl.username'         : 'cdupjrrn',
                'sasl.password'         : 'wOu38-uSQgwUokH4pzEcrIy5PjECFoXd'}

# Helper functions

def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True


# JSON Data Storing - If data are JSON formatted then the parsing algorithm is applied and store them accordingly

def store_json_formatted_data(data,conn):
    try:
        kafka_payload_dict = json.loads(data)
        kafka_payload_columns = list(kafka_payload_dict.keys()) 
        kafka_payload_dict['KAFKA_TIME'] = kafka_datetime
              
        kafka_payload_dict = dict((k.upper(), v) for k, v in kafka_payload_dict .items())
        kafka_payload_dict = dict((k.replace(" ",""), v) for k, v in kafka_payload_dict .items()) 
        kafka_payload_dict['PAYLOAD'] = dict((k.upper(), v) for k, v in kafka_payload_dict['PAYLOAD'] .items()) 
        kafka_payload_dict['PAYLOAD'] = dict((k.replace(" ",""), v) for k, v in kafka_payload_dict['PAYLOAD'] .items())    

        table_name = (kafka_payload_dict['DEVICEID'] + """_""" + kafka_payload_dict['TYPE']).replace(" ", "")
      
        cursor = conn.cursor()

        warnings.filterwarnings("ignore")
                    
        # Create table if not exist
        create_table = "CREATE TABLE IF NOT EXISTS " + table_name + " (ID int NOT NULL AUTO_INCREMENT,PRIMARY KEY (ID), DB_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, KAFKA_TIME varchar(255), DEVICEID varchar(255) NOT NULL );"
        cursor.execute(create_table)
        conn.commit()

        # Create columns
        if len(kafka_payload_dict['PAYLOAD']) != 0:
            query_create_columns = "ALTER TABLE " + table_name+" "+", ".join(["ADD COLUMN IF NOT EXISTS " + xval + " VARCHAR(255) " for xval in kafka_payload_dict['PAYLOAD']])
            cursor.execute(query_create_columns)
            conn.commit()
        else:

            store_raw_data(data,conn)
            print("[JSON][ER] No payload inside json file! The data moved in to RAW Data")

        # Insert data                   
        placeholder = ("%s,"*len(kafka_payload_dict['PAYLOAD']))+"%s,%s"
        query_insert_data_values = [kafka_payload_dict['DEVICEID'],kafka_payload_dict['KAFKA_TIME']] + list(kafka_payload_dict['PAYLOAD'].values())
        query_insert_data = "insert into `{table}` ({columns}) values ({values});".format(table=table_name, columns="DEVICEID,KAFKA_TIME,"+",".join(kafka_payload_dict['PAYLOAD'].keys()), values=placeholder)

        cursor.execute(query_insert_data, query_insert_data_values)
        conn.commit()    
        
    except:
        return False
    return True

# Raw Data Storing - If not a JSON the app will store the data in a raw format

def store_raw_data(data,conn):
    try:
        cursor = conn.cursor()

        warnings.filterwarnings("ignore")
                    
        # Create table if not exist
        create_table = "CREATE TABLE IF NOT EXISTS raw_data (ID int NOT NULL AUTO_INCREMENT,PRIMARY KEY (ID), DB_TIME TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, KAFKA_TIME varchar(255), DATA varchar(4096) NOT NULL );"
        cursor.execute(create_table)
        conn.commit()

        # Insert data
        cursor.execute("""INSERT INTO raw_data (data, KAFKA_TIME) VALUES (%s,%s) """,(data, kafka_datetime) )
        connection.commit() 

    except:
        return False
    return True

# MAIN APPLICATION (LOOP-FOREVER)

if __name__ == '__main__':
    topics = 'cdupjrrn-test01'.split(",")

    # https://api.cloudkarafka.com/console/d0406d93-e88e-444d-9b96-cb90cefcd1fc/browser

    c = Consumer(**kafka_config)
    c.subscribe(topics)
 
    try:
        while True:
            msg = c.poll(timeout=1.0)   

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
                    connection.close()
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:

                # Proper message

                print("[KFKQ][NW] "+str(msg.topic())+"(prt:"+str(msg.partition())+" +:"+str(msg.offset())+" key:"+str(msg.key())+")" )
                                             
                #################################################################################################

                # database connection
                connection = pymysql.connect(database_config['hostname'], database_config['username'], database_config['password'], database_config['database'] )
        
                # Kafka-data/time
                kafka_payload = msg.value().decode('utf-8') 
                kafka_datetime = time.strftime('%m/%d/%Y %H:%M:%S',  time.gmtime(msg.timestamp()[1]/1000.))
                
                if is_json(kafka_payload) == True:
                    if store_json_formatted_data(kafka_payload,connection) == True:
                        print("[JSON][OK] New entry")               
                    else:
                        print("[JSON][ER] Wrong entry format")
                else:
                    if store_raw_data(kafka_payload,connection) == True:
                        print("[RAWD][OK] New entry")               
                    else:
                        print("[RAWD][ER] New entry")

                connection.close()
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    # Close down consumer to commit final offsets.
    c.close()