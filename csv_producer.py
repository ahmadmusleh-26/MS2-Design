from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file
import json
import csv
import os
import time

# Search the current directory for the JSON file (including the service account key)
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "eda-project-509219"   # <-- change to your project ID
mysql_topic_name = "Design2MySQL"   # <-- the topic the MySQL sink connector (design-integration) listens to
redis_topic_name = "Design2Redis"   # <-- the topic the Redis sink connector (design-redis-integration) listens to
csv_file = "Labels.csv"

# Columns that hold numbers. The CSV reader returns every value as a string,
# but the MySQL connector expects real numbers for the DOUBLE columns.
numeric_columns = ["time", "temperature", "humidity", "pressure"]

# create a publisher and get the topic paths for the publisher
# (message ordering is enabled so an ordering key can be attached, the Redis connector uses it as the Redis key)
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
mysql_topic_path = publisher.topic_path(project_id, mysql_topic_name)
redis_topic_path = publisher.topic_path(project_id, redis_topic_name)
print(f"Publishing records from {csv_file} to {mysql_topic_path} and {redis_topic_path}.\n")

# Open and read the CSV file
with open(csv_file, newline='') as f:
    reader = csv.DictReader(f)   # each row becomes a dictionary, keyed by the header row

    # enumerate gives each row a number starting at 1, used as the ID (primary key) in MySQL
    for row_id, row in enumerate(reader, start=1):
        # row is a dictionary here, e.g.:
        # {'time': '1768708698.49', 'profileName': 'denver', 'temperature': '31.11', ...}

        # Replace empty strings (missing values in the CSV) with None,
        # so they are serialized as JSON null and stored as NULL in MySQL.
        # Convert the numeric columns from strings to floats.
        record = {"ID": row_id}
        for key, value in row.items():
            if value == '':
                record[key] = None
            elif key in numeric_columns:
                record[key] = float(value)
            else:
                record[key] = value

        # Serialize the dictionary into a JSON string, then encode it to bytes
        # (Pub/Sub messages must be sent as bytes, not as a Python object or plain JSON string)
        message = json.dumps(record).encode('utf-8')

        print("Producing a record:", message)

        # 1) MySQL topic: the connector stores the record as a new row in the Labels table
        mysql_future = publisher.publish(mysql_topic_path, message)

        # 2) Redis topic: the connector stores the record as a value under the key "record:<ID>"
        redis_future = publisher.publish(redis_topic_path, message, ordering_key=f"record:{row_id}")

        # ensure that the publishing has been completed successfully
        mysql_future.result()
        redis_future.result()

        time.sleep(0.2)   # small delay so the records can be seen arriving one by one in the consumer

print("\nAll records have been published.")
