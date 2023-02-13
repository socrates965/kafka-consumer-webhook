import os
import requests
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Set the consumer configuration
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVER')
security_protocol = os.getenv('SECURITY_PROTOCOL')
sasl_mechanism = os.getenv('SASL_MECHANISM')
sasl_plain_username = os.getenv('KAFKA_USERNAME')
sasl_plain_password = os.getenv('KAFKA_PASSWORD')
topic = os.getenv('KAFKA_TOPIC')
url = os.getenv('URL')

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    security_protocol=security_protocol,
    sasl_mechanism=sasl_mechanism,
    sasl_plain_username=sasl_plain_username,
    sasl_plain_password=sasl_plain_password,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Subscribe to a topic
consumer.subscribe(topics=[topic])
headers = {
    "Content-Type": "application/json"
}

# Continuously poll for new messages
while True:
    try:
        msg = consumer.poll(timeout_ms=1000)
    except KafkaError as error:
        print(f"Error: {error}")
        break
    else:
        for topic_partition, messages in msg.items():
            for message in messages:
                # Do something with the message
                # print(f"Received message: {message.value['txt']}")
                counter=message.value['txt'].split()[-1]
                payload = {
                    "firstName": "firstName"+str(counter),
                    "lastName": "",
                    "id": counter,
                    "position": "Developer",
                    "emailId": counter
                }
                # print(payload)
                response = requests.post(url, json=payload, headers=headers)
                data = response.json()
                print(data)
                # print(f"Received message: {message.value}")

# Close the consumer
consumer.close()
