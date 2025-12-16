# Write your json consumer code here
from kafka import KafkaConsumer
import json

print("Starting JSON consumer...")

# Créer le consumer avec désérialisation JSON automatique
consumer = KafkaConsumer(
    'transactions_json',
    bootstrap_servers='localhost:9092',
    group_id='json-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started. Waiting for JSON messages...")

# Lire les messages
for message in consumer:
    try:
        # Le message est déjà un dictionnaire Python
        data = message.value
        """
        # Afficher les informations
        print(f"Partition: {message.partition} | Offset: {message.offset}")
        print(f"  Transaction ID: {data['transaction_id']}")
        print(f"  User ID: {data['user_id']}")
        print(f"  Amount: ${data['amount']:.2f}")
        print(f"  Timestamp: {data['timestamp']}")
        print("-" * 50)"""
        print (data)
        
    except (json.JSONDecodeError, KeyError) as e:
        # Gérer les messages malformés
        print(f"ERROR - Malformed message at offset {message.offset}: {e}")
        print(f"Raw value: {message.value}")
