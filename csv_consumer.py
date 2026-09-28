import pymysql      # pip install pymysql  ##to install
import redis        # pip install redis    ##to install
import json
import time

# MySQL and Redis servers deployed on GKE (EXTERNAL-IPs from: kubectl get services)
mysql_ip = "34.130.119.2"      # <-- change to the EXTERNAL-IP of mysql-service
redis_ip = "34.130.232.118"    # <-- change to the EXTERNAL-IP of redis
password = "sofe4630u"         # same password for both servers (see the YAML files)
database = "Readings"
table = "Labels"

# connect to the MySQL server
mysql_connection = pymysql.connect(host=mysql_ip, port=3306, user="usr", password=password,
                                   database=database, cursorclass=pymysql.cursors.DictCursor, autocommit=True)

# connect to the Redis server (database 0)
r = redis.Redis(host=redis_ip, port=6379, db=0, password=password)

print(f"Reading records stored in MySQL ({database}.{table}) and in Redis (keys record:<ID>)..\n")

# remember what was already consumed, so each record is only printed once.
# Sets are used (not just the last ID) because Pub/Sub may deliver the messages out of order,
# so a record with a smaller ID can be stored after a record with a bigger ID.
seen_mysql_ids = set()
seen_redis_keys = set()

try:
    while True:
        # --- MySQL: get the rows that were not consumed yet, in order of their ID ---
        with mysql_connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table} ORDER BY ID")
            rows = [row for row in cursor.fetchall() if row["ID"] not in seen_mysql_ids]

        for row in rows:
            print(f"Consumed from MySQL: {row}")
            seen_mysql_ids.add(row["ID"])

        # --- Redis: get the keys that were not consumed yet, in order of their ID ---
        new_keys = [k.decode() for k in r.keys("record:*") if k.decode() not in seen_redis_keys]
        new_keys.sort(key=lambda k: int(k.split(":")[1]))

        for key in new_keys:
            record = json.loads(r.get(key))
            print(f"Consumed from Redis: {key} -> {record}")
            seen_redis_keys.add(key)

        time.sleep(1)   # check both storages for new records every second
except KeyboardInterrupt:
    mysql_connection.close()
