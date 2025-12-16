# Write your csv consumer code here
from kafka import KafkaConsumer

# Créer le consumer
consumer = KafkaConsumer(
    'test_clean',
    bootstrap_servers='localhost:9092',
    group_id='csv-consumer-group',
    auto_offset_reset='earliest'
)

print("Consumer started. Waiting for messages...")

# Lire les messages en continu
for message in consumer:
    # Décoder le message
    data = message.value.decode('utf-8')
    
    # Afficher les informations
    print(f"Partition: {message.partition} | Offset: {message.offset} | Data: {data.strip()}")
