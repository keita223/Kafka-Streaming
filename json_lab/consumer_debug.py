from kafka import KafkaConsumer
import json

print("=== DEBUG CONSUMER - Replaying historical data ===")

# Demander à l'utilisateur quel offset rejouer
topic = input("Enter topic name (transactions_raw, transactions_high_value): ")
start_offset = int(input("Enter starting offset (0 for beginning): "))

# Créer le consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers='localhost:9092',
    group_id='debug-consumer',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"\nReplaying messages from topic '{topic}' starting at offset {start_offset}...\n")

count = 0
for message in consumer:
    # Filtrer selon l'offset demandé
    if message.offset >= start_offset:
        data = message.value
        print(data)
        
     
        
        # Détecter des anomalies potentielles
        if data['amount'] > 450:
            print("  ⚠️  WARNING: Very high transaction amount!")
        
        print("-" * 60)
        count += 1
        
        # Limiter à 10 messages pour le démo
        if count >= 10:
            print(f"\n✅ Replayed {count} messages. Press Ctrl+C to stop or wait for more...")
            count = 0
