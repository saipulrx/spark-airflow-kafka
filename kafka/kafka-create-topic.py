from kafka.admin import KafkaAdminClient, NewTopic
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer

dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv('KAFKA_HOST')
def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1, bootstrap_servers=f'{kafka_host}:9092'):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='kafka-topic-creator'
        )
        
        topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
    #finally:
    #    if admin_client:  # Check if admin_client is initialized before closing
    #        admin_client.close()

if __name__ == "__main__":
    topic_name = "my_new_topic"
    create_kafka_topic(topic_name, num_partitions=3, replication_factor=1)