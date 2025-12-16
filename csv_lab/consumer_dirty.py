from kafka import KafkaConsumer
import json

print(" Consumer started - Reading DIRTY CSV transactions\n")

TOPIC = "transactions_dirty"
BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="dirty-csv-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Compteurs
total_count = 0
clean_count = 0
cleaned_count = 0

print("📊 Reading transactions from Kafka...\n")

for message in consumer:
    data = message.value
    total_count += 1
    
    # Vérifier si le message a été nettoyé
    was_cleaned = data.get("was_cleaned", False)
    
    if was_cleaned:
        cleaned_count += 1
        status = " CLEANED"
    else:
        clean_count += 1
        status = " CLEAN"
    
    # Afficher le message
    print(data)
    
    
    # Afficher stats tous les 5 messages
    if total_count % 5 == 0:
        print("\n" + "=" * 70)
        print(f" STATISTICS (after {total_count} messages)")
        print(f"    Clean records: {clean_count}")
        print(f"    Cleaned records: {cleaned_count}")
        if total_count > 0:
            print(f"   Data quality: {(clean_count/total_count*100):.1f}%")
      
