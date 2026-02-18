import sys

from fastavro import reader

reader_schema = {
    "type": "record",
    "name": "UnifiedTransaction",
    "aliases": ["TransactionV1", "TransactionV2"],
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "event_time", "type": "string"},
        {"name": "total_amount", "type": "string", "aliases": ["amount"]},
        {"name": "user_email", "type": "string", "default": ""},
        {"name": "currency", "type": "string", "default": ""},
    ],
}

if len(sys.argv) != 2:
    print("Usage: python unified_reader.py <avro_file>")
    sys.exit(1)

avro_file = sys.argv[1]

print("id,event_time,total_amount,user_email,currency")

with open(avro_file, 'rb') as f:
    avro_reader = reader(f, reader_schema)
    for record in avro_reader:
        print(
            f"{record['id']},{record['event_time']},{record['total_amount']},{record['user_email']},{record['currency']}")
