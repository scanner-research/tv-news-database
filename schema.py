from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
import os

Base = declarative_base()

class Face(Base):
	__tablename__ = 'face'
	id = Column(Integer, primary_key=True)
	bbox_x1 = Column(Float)
	bbox_x2 = Column(Float)
	bbox_y1 = Column(Float)
	bbox_y2 = Column(Float)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	shot_id = Column(Integer)
	background = Column(Boolean)
	is_host = Column(Boolean)
	blurriness = Column(Float)
	probability = Column(Float)
	frame_id = Column(Integer, ForeignKey('frame.id'))

class Gender(Base):
	__tablename__ = 'gender'
	id = Column(Integer, primary_key=True)
	name = Column(String(1))

class CanonicalShow(Base):
	__tablename__ = 'canonical_show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	is_recurring = Column(Boolean)

class Show(Base):
	__tablename__ = 'show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'))

class Video(Base):
	__tablename__ = 'video'
	id = Column(Integer, primary_key=True)
	path = Column(String)
	num_frames = Column(Integer)
	fps = Column(Float)
	width = Column(Integer)
	height = Column(Integer)
	time = Column(DateTime)
	channel_id = Column(Integer, ForeignKey('channel.id'))
	show_id = Column(Integer, ForeignKey('show.id'))
	has_captions = Column(Boolean)
	commercials_labeled = Column(Boolean)
	srt_extension = Column(String)
	threeyears_dataset = Column(Boolean)
	duplicate = Column(Boolean)
	corrupted = Column(Boolean)

class Labeler(Base):
	__tablename__ = 'labeler'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	created = Column(DateTime)
	data_path = Column(String)

class Channel(Base):
	__tablename__ = 'channel'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class Frame(Base):
	__tablename__ = 'frame'
	id = Column(Integer, primary_key=True)
	number = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))
	shot_boundary = Column(Boolean)
	sampler_id = Column(Integer, ForeignKey('frame_sampler.id'))

class FrameSampler(Base):
	__tablename__ = 'frame_sampler'
	id = Column(Integer, primary_key=True)
	name = Column(String)

if __name__ == '__main__':
	password = os.getenv("POSTGRES_PASSWORD")
	engine = create_engine("postgresql://admin:{}@localhost/postgres".format(password))

	conn = psycopg2.connect(dbname="postgres", user="admin", host='localhost', password=password)
	cur = conn.cursor()
	
	# Drop the old tables so we can recreate the schema.
	# This order must obey foreign key dependencies
	tables = ['gender', 'canonical_show', 'show', 'channel', 'video', 'labeler', 'frame_sampler']
	for table in tables:
		print("Dropping", table)
		cur.execute("DROP TABLE IF EXISTS public.{} CASCADE;".format(table));
	conn.commit()
	
	# Create the tables
	for clazz in [Face, Gender, CanonicalShow, Show, Video, Labeler, Channel, Frame, FrameSampler]:
		clazz.metadata.create_all(engine)

	# Populate the tables with data
	for table in tables:
		print("loading", table)
		fd = open("/newdisk/pg/query_{}.csv".format(table.replace("_", "")))
		fd.readline() # Skip the headers
		cur.copy_from(fd, table, sep=",", null="")
		conn.commit()

	cur.close()
	conn.close()
