import psycopg2
import os
from sqlalchemy import create_engine
from schema import *
import inspect

clsmembers = inspect.getmembers(sys.modules["schema"], inspect.isclass)
print(clsmembers)

# password = os.getenv("POSTGRES_PASSWORD")
# engine = create_engine("postgresql://admin:{}@localhost/postgres".format(password))

# conn = psycopg2.connect(dbname="postgres", user="admin", host='localhost', password=password)
# cur = conn.cursor()

# # Drop the old tables so we can recreate the schema.
# # This order must obey foreign key dependencies
# tables = ['gender', 'canonical_show', 'show', 'channel', 'video', 'labeler', 'frame_sampler', 'identity', 'face_identity', 'show_hosts']
# for table in tables:
# 	print("Dropping", table)
# 	cur.execute("DROP TABLE IF EXISTS public.{} CASCADE;".format(table));
# conn.commit()

# # Create the tables
# for clazz in [Face, Gender, CanonicalShow, Show, Video, Labeler, Channel, Frame, FrameSampler, Identity, FaceIdentity, ShowHosts]:
# 	clazz.metadata.create_all(engine)

# # Populate the tables with data
# for table in tables:
# 	print("loading", table)
# 	fd = open("/newdisk/pg/query_{}.csv".format(table.replace("_", "")))
# 	fd.readline() # Skip the headers
# 	cur.copy_from(fd, table, sep=",", null="")
# 	conn.commit()

# cur.close()
# conn.close()
