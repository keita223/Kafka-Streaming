#write your json producer code here
from kafka import KafkaProducer
import json
import csv
import time

print("Starting JSON producer...")

# Créer le producer avec sérialisation JSON automatique
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Producer created successfully!")

# Ouvrir et lire le fichier CSV
filepath='transactions.csv'
with open(filepath, 'r') as file:
    csv_reader = csv.DictReader(file)
    
    for row in csv_reader:
        # Créer l'objet JSON avec les bons types + nouveaux champs
        json_message = {
            "transaction_id": int(row['transaction_id']),
            "user_id": int(row['user_id']),
            "amount": float(row['amount']),
            "timestamp": row['timestamp'],
            # NOUVEAUX CHAMPS
            "currency": "USD",
            "status": "completed",
            "version": 2
        }
        
        # Envoyer au topic raw (toutes les transactions)
        producer.send('transactions_raw', json_message)

        # Si montant > 300, envoyer aussi au topic high_value
        if json_message['amount'] > 300:
            producer.send('transactions_high_value', json_message)
            print(f"Sent to HIGH VALUE: {json_message}")
        else:
            print(f"Sent to RAW only: {json_message}")
        
        # Délai pour simuler streaming
        time.sleep(0.5)

# Fermer 
producer.close()
print("JSON Producer finished!")
