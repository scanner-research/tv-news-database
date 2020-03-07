import psycopg2
import os
from sqlalchemy import create_engine, MetaData
import schema
import inspect
import sys
import time

tables = list(map(str, schema.Face.metadata.sorted_tables))

# These are really big to reimport
exclude = [] # 'face', 'frame', 'commercial', 'segment']

password = os.getenv("POSTGRES_PASSWORD")
engine = create_engine("postgresql://admin:{}@localhost/postgres".format(password))

conn = psycopg2.connect(dbname="tvnews", user="admin", host='localhost', password=password)
cur = conn.cursor()

# Drop the old tables so we can recreate the schema.
# Reverse the order to avoid violating dependencies.
# MetaData().drop_all(engine)
# for table in reversed(tables):
# 	if table in exclude:
# 		continue
# 	print("Dropping", table)
# 	cur.execute("DROP TABLE IF EXISTS public.{};".format(table));
# conn.commit()

# Create the tables. The metadata object of a single table is aware of all
# tables, and knows the order in which to create them.
schema.Face.metadata.create_all(engine)

# Populate the tables with data. They are sorted in dependency order.
for table_name in tables:
	if table_name in exclude:
		continue
	pretime = time.time()
	print("Loading", table_name)
	fd = open("/newdisk/trimmed/{}.csv".format(table_name))
	headers = fd.readline() # Skip the headers
	print(headers)
	try:
		cur.copy_expert("copy {}({}) from stdin (format csv)".format(table_name, headers), fd)
		conn.commit()
		print("Loaded {} in {:.3f} seconds".format(table_name, time.time() - pretime))
	except psycopg2.errors.UniqueViolation as e:
		print("Duplicate key", e)
		conn.rollback()

cur.close()
conn.close()
