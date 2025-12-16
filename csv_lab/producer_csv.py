#Write your csv producer code here
from kafka import KafkaProducer
import time

# Create  le producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Ouvrir le fichier CSV
with open('data/transactions.csv', 'r') as file:
    # Pour chaque ligne du fichier
    for line in file:
        # Encoder la ligne en bytes
        message = line.encode('utf-8')
        
        # Envoye au topic
        producer.send('test_clean', message)
        
        # Afficher pour voir ce qui est envoyé
        print(f"Sent: {line.strip()}")
        
        # Délai pour simuler streaming
        time.sleep(0.5)

# Fermer proprement
producer.close()
print("Producer finished!")
