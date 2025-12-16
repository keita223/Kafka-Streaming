from kafka import KafkaConsumer
import json

print(" Consumer started - Reading DIRTY JSON transactions\n")

TOPIC = "transactions_json_dirty"
BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="dirty-json-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

total_count = 0
clean_count = 0
cleaned_count = 0

print(" Reading JSON transactions...\n")

for message in consumer:
    data = message.value
    total_count += 1
    
    # Vérifier qualité
    quality_info = data.get("data_quality", {})
    was_cleaned = quality_info.get("was_cleaned", False)
    
    if was_cleaned:
        cleaned_count += 1
        status = "🧹 CLEANED"
        print(f"{status} - Quality Score: {quality_info.get('quality_score', 0):.2f}")
        print(f"   Cleaned fields: {quality_info.get('cleaned_fields', [])}")
    else:
        clean_count += 1
        status = "PRISTINE"
        print(data)
    
    if total_count % 5 == 0:
        print("\n" + "="*70)
        print(f"📊 STATISTICS (after {total_count} messages)")
        print(f"   ✅ Pristine: {clean_count}")
        print(f"   🧹 Cleaned: {cleaned_count}")
        print(f"   Quality rate: {(clean_count/total_count*100):.1f}%")
        print("="*70 + "\n")
