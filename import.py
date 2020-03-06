import psycopg2
import os
from sqlalchemy import create_engine, MetaData
import schema
import inspect
import sys
import time

tables = list(map(str, schema.Face.metadata.sorted_tables))

# These are really big to reimport
exclude = ['face', 'frame', 'commercial']

password = os.getenv("POSTGRES_PASSWORD")
engine = create_engine("postgresql://admin:{}@localhost/postgres".format(password))

conn = psycopg2.connect(dbname="postgres", user="admin", host='localhost', password=password)
cur = conn.cursor()

# Drop the old tables so we can recreate the schema.
# MetaData().drop_all(engine)
for table in tables:
	if table in exclude:
		continue
	print("Dropping", table)
	cur.execute("DROP TABLE IF EXISTS public.{} CASCADE;".format(table));
conn.commit()

# Create the tables. The metadata object of a single table is aware of all
# tables, and knows the order in which to create them.
schema.Face.metadata.create_all(engine)

# Populate the tables with data
for table_name in tables:
	if table_name in exclude:
		continue
	pretime = time.time()
	print("Loading", table_name)
	fd = open("/newdisk/pg/query_{}.csv".format(table_name))
	fd.readline() # Skip the headers
	cur.copy_expert("copy {} from stdin (format csv)".format(table_name), fd)
	conn.commit()
	print("Loaded {} in {:.3f} seconds".format(table_name, time.time() - pretime))

cur.close()
conn.close()
