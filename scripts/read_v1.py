from fastavro import reader

print("id,event_time,user_email,amount")

with open("output/transactions_v1.avro", 'rb') as f:
    avro_reader = reader(f)
    for record in avro_reader:
        print(
            f"{record['id']},{record['event_time']},{record['user_email']},{record['amount']}")
