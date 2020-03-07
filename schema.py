from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CanonicalShow(Base):
	__tablename__ = 'canonical_show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	is_recurring = Column(Boolean)
	channel_id = Column(Integer, ForeignKey('channel.id'))

	UniqueConstraint('unique_name_channel', name, channel_id)

class Show(Base):
	__tablename__ = 'show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'))
	channel_id = Column(Integer, ForeignKey('channel.id'))
	
	UniqueConstraint('unique_name_channel', name, channel_id)

class Channel(Base):
	__tablename__ = 'channel'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True)

class Video(Base):
	__tablename__ = 'video'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True)
	num_frames = Column(Integer)
	fps = Column(Float)
	width = Column(Integer)
	height = Column(Integer)
	time = Column(DateTime)
	show_id = Column(Integer, ForeignKey('show.id'))
	is_duplicate = Column(Boolean)
	is_corrupt = Column(Boolean)

class FrameSampler(Base):
	__tablename__ = 'frame_sampler'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class Frame(Base):
	__tablename__ = 'frame'
	id = Column(Integer, primary_key=True)
	number = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))
	sampler_id = Column(Integer, ForeignKey('frame_sampler.id'))

class Labeler(Base):
	__tablename__ = 'labeler'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True)
	created = Column(DateTime)
	is_handlabel = Column(Boolean)
	comments = Column(String)

class Face(Base):
	__tablename__ = 'face'
	id = Column(Integer, primary_key=True)
	bbox_x1 = Column(Float)
	bbox_x2 = Column(Float)
	bbox_y1 = Column(Float)
	bbox_y2 = Column(Float)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	score = Column(Float)
	frame_id = Column(Integer, ForeignKey('frame.id'))

class Identity(Base):
	__tablename__ = 'identity'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True)
	is_ignore = Column(Boolean)

class FaceIdentity(Base):
	__tablename__ = 'face_identity'
	face_id = Column(Integer, ForeignKey('face.id'), primary_key=True)
	labeler_id = Column(Integer, ForeignKey('labeler.id'), primary_key=True)
	score = Column(Float)
	identity_id = Column(Integer, ForeignKey('identity.id'))

class Gender(Base):
	__tablename__ = 'gender'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True)

class FaceGender(Base):
	__tablename__ = 'face_gender'
	face_id = Column(Integer, ForeignKey('face.id'), primary_key=True)
	gender_id = Column(Integer, ForeignKey('gender.id'), primary_key=True)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	score = Column(Float)

class Commercial(Base):
	__tablename__ = 'commercial'
	id = Column(Integer, primary_key=True)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	max_frame = Column(Integer)
	min_frame = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))

class HostsAndStaff(Base):
	__tablename__ = 'hosts_and_staff'
	channel_id = Column(Integer, ForeignKey('channel.id'), primary_key=True)
	canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'), primary_key=True)
	identity_id = Column(Integer, ForeignKey('identity.id'), primary_key=True)
