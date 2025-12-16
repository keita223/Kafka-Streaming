import csv
import json
import time
from kafka import KafkaProducer
from datetime import datetime

TOPIC = "transactions_json"
BOOTSTRAP_SERVERS = "localhost:9092"
CSV_PATH = "data/transactions_dirty.csv"

DEFAULT_CURRENCY = "EUR"
VALID_CURRENCIES = {"USD", "EUR", "TND"}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

total_amount = 0.0
count_amount = 0


def compute_mean():
    return round(total_amount / count_amount, 2) if count_amount > 0 else None


def parse_amount(raw_amount):
    global total_amount, count_amount
    try:
        amount = float(raw_amount)
        if amount <= 0:
            raise ValueError
        total_amount += amount
        count_amount += 1
        return amount
    except (TypeError, ValueError):
        mean = compute_mean()
        if mean is not None:
            print(f"Amount imputé par la moyenne: {mean}")
            return mean
        return None


def normalize_currency(raw_currency):
    if not raw_currency:
        return DEFAULT_CURRENCY
    currency = raw_currency.upper().strip()
    return currency if currency in VALID_CURRENCIES else DEFAULT_CURRENCY


while True:
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if not row.get("transaction_id"):
                print(" Ligne rejetée (transaction_id manquant):", row)
                continue

            amount = parse_amount(row.get("amount"))
            if amount is None:
                print("Ligne rejetée (pas de moyenne dispo):", row)
                continue

            event = {
                "transaction_id": row["transaction_id"].strip(),
                "amount": amount,
                "currency": normalize_currency(row.get("currency")),
                "timestamp": row.get("timestamp") or datetime.utcnow().isoformat()
            }

            producer.send(TOPIC, event)
            print(" Envoyé:", event)
            time.sleep(1)

    time.sleep(5)