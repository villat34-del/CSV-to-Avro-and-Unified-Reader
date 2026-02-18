import csv

from fastavro import writer

schema = {
    "type": "record",
    "name": "TransactionV2",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "event_time", "type": "string"},
        {"name": "total_amount", "type": "string"},
        {"name": "currency", "type": "string"},
    ],
}

records = []
with open("data/transactions_v2.csv", 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile)
    for row in csv_reader:
        record = {
            "id": int(row["id"]),
            "event_time": row["event_time"],
            "total_amount": row["total_amount"],
            "currency": row["currency"],
        }
        records.append(record)

with open("output/transactions_v2.avro", 'wb') as out:
    writer(out, schema, records)

print("Avro file 'transactions_v2.avro' has been created successfully.")
