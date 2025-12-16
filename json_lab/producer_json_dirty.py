from kafka import KafkaProducer
import csv
import json
import time
from datetime import datetime

print("Starting JSON DIRTY producer with data cleaning...")

TOPIC = "transactions_json_dirty"
BOOTSTRAP_SERVERS = "localhost:9092"


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ÉTAPE 1 : Calculer les statistiques
print("📊 Calculating statistics...")

amounts = []
user_ids = []

with open('transactions_dirty.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        try:
            amount = float(row['amount'])
            amounts.append(amount)
        except:
            pass
        
        try:
            user_id = int(row['user_id'])
            user_ids.append(user_id)
        except:
            pass

# Calculer moyenne et mode
if amounts:
    average_amount = sum(amounts) / len(amounts)
else:
    average_amount = 0.0

if user_ids:
    most_common_user = max(set(user_ids), key=user_ids.count)
else:
    most_common_user = 100

print(f"✅ Average amount: ${average_amount:.2f}")
print(f"✅ Most common user: {most_common_user}")
print("\n🧹 Cleaning and sending JSON data...\n")

# ÉTAPE 2 : Nettoyer et envoyer
clean_count = 0
dirty_count = 0
last_valid_timestamp = None

with open('transactions_dirty.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    
    for row in csv_reader:
        cleaned_fields = []
        
        # Nettoyer transaction_id
        try:
            transaction_id = int(row['transaction_id'])
        except:
            transaction_id = -1
            cleaned_fields.append("transaction_id")
        
        # Nettoyer user_id (utiliser le mode)
        try:
            user_id = int(row['user_id'])
        except:
            user_id = most_common_user
            cleaned_fields.append("user_id")
        
        # Nettoyer amount (utiliser la moyenne)
        try:
            amount = float(row['amount'])
        except:
            amount = average_amount
            cleaned_fields.append("amount")
        
        # Nettoyer timestamp (forward fill)
        timestamp = row.get('timestamp', '')
        if timestamp is None:
            timestamp = ''
        timestamp = str(timestamp).strip()
        
        if not timestamp or timestamp == 'not-a-date':
            if last_valid_timestamp:
                timestamp = last_valid_timestamp
            else:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cleaned_fields.append("timestamp")
        else:
            last_valid_timestamp = timestamp
        
        # Créer le message JSON enrichi
        message = {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "amount": amount,
            "timestamp": timestamp,
            # Champs JSON supplémentaires
            "currency": "USD",
            "status": "completed",
            "version": 2,
            # Métadonnées de qualité
            "data_quality": {
                "was_cleaned": len(cleaned_fields) > 0,
                "cleaned_fields": cleaned_fields,
                "quality_score": 1.0 - (len(cleaned_fields) / 4)
            }
        }
        
        # Envoyer
        producer.send(TOPIC, message)
        
        if cleaned_fields:
            dirty_count += 1
            print(f"🧹 CLEANED: {message}")
        else:
            clean_count += 1
            print(f"✅ CLEAN: {message}")
        
        time.sleep(0.3)

producer.close()

print("\n" + "="*60)
print(f"✅ Clean records: {clean_count}")
print(f"🧹 Cleaned records: {dirty_count}")
print(f"📊 Total: {clean_count + dirty_count}")
print(f"🎯 Data quality: {(clean_count/(clean_count + dirty_count)*100):.1f}%")
print("="*60)
