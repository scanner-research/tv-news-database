from sqlalchemy import create_engine
import sqlalchemy
import os
import csv
import schema
from sqlalchemy.orm import sessionmaker

password = os.getenv("POSTGRES_PASSWORD")
engine = create_engine("postgresql://admin:{}@localhost/postgres".format(password))

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# create a Session
session = Session()

with open('/newdisk/pg/query_face_gender.csv') as csvfile:
	csvfile.readline() # Skip the headers
	csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
	for i, row in enumerate(csvreader):
		if i % 100 == 0:
			print(i)
		dbrow = schema.FaceGender(id=row[0], face_id=row[1], gender_id=row[2], labeler_id=row[3], score=row[4])
		print(dbrow)
		session.add(dbrow)
		session.flush()
