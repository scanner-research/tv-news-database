import psycopg2
import os
from sqlalchemy import create_engine, MetaData
from schema import *
import inspect
import sys

tables = inspect.getmembers(sys.modules["schema"],
	lambda member: inspect.isclass(member) and member.__module__ == "schema")

# These are really big and painful to reimport
exclude = ['Face', 'Frame']

password = os.getenv("POSTGRES_PASSWORD")
engine = create_engine("postgresql://admin:{}@localhost/postgres".format(password))

conn = psycopg2.connect(dbname="postgres", user="admin", host='localhost', password=password)
cur = conn.cursor()

# Drop the old tables so we can recreate the schema.
# MetaData().drop_all(engine)
for class_name, table in tables:
	# These are really big and painful to reimport
	if class_name in exclude:
		continue
	print("Dropping", table)
	cur.execute("DROP TABLE IF EXISTS public.{} CASCADE;".format(table.__tablename__));
conn.commit()

# Create the tables. The metadata object of a single table is aware of all
# tables, and knows the order in which to create them.
tables[0][1].metadata.create_all(engine)

# Populate the tables with data
for class_name, table in tables:
	if class_name in exclude:
		continue
	table_name = table.__tablename__
	print("Loading", table_name)
	fd = open("/newdisk/pg/query_{}.csv".format(table_name))
	fd.readline() # Skip the headers
	cur.copy_from(fd, table_name, sep=",", null="")
	conn.commit()

cur.close()
conn.close()
