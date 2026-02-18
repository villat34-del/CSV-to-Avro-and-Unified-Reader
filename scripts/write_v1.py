import csv

from fastavro import writer

schema = {
    "type": "record",
    "name": "TransactionV1",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "event_time", "type": "string"},
        {"name": "user_email", "type": "string"},
        {"name": "amount", "type": "string"}
    ],
}

records = []
with open("data/transactions_v1.csv", 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
        record = {
            "id": int(row["id"]),
            "event_time": row["event_time"],
            "user_email": row["user_email"],
            "amount": row["amount"]
        }
        records.append(record)
with open("output/transactions_v1.avro", 'wb') as out:
    writer(out, schema, records)

print("Avro file 'transactions_v1.avro' has been created successfully.")
