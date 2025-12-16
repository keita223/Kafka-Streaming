from kafka import KafkaConsumer
import json

print("Starting HIGH VALUE consumer...")

# Créer le consumer pour les transactions à haute valeur
consumer = KafkaConsumer(
    'transactions_high_value',
    bootstrap_servers='localhost:9092',
    group_id='high-value-consumer-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started. Waiting for HIGH VALUE transactions (> $300)...")

# Lire les messages
for message in consumer:
    try:
        data = message.value
        print(data)
        
        
    except (json.JSONDecodeError, KeyError) as e:
        print(f"ERROR - Malformed message: {e}")
