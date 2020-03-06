from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

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

class Identity(Base):
	__tablename__ = 'identity'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class FaceIdentity(Base):
	__tablename__ = 'face_identity'
	id = Column(Integer, primary_key=True)
	face_id = Column(Integer, ForeignKey('face.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	probability = Column(Float)
	identity_id = Column(Integer, ForeignKey('identity.id'))

class ShowHosts(Base):
	__tablename__ = 'show_hosts'
	id = Column(Integer, primary_key=True)
	show_id = Column(Integer, ForeignKey('show.id'))
	identity_id = Column(Integer, ForeignKey('identity.id'))
