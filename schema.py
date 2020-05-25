from sqlalchemy import *
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CanonicalShow(Base):
	__tablename__ = 'canonical_show'
	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)
	is_recurring = Column(Boolean, default=False)
	channel_id = Column(Integer, ForeignKey('channel.id'), nullable=False)

	UniqueConstraint('unique_name_channel', name, channel_id)

class Show(Base):
	__tablename__ = 'show'
	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)
	canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'), nullable=False)
	channel_id = Column(Integer, ForeignKey('channel.id'), nullable=False)

	UniqueConstraint('unique_name_channel', name, channel_id)

class Channel(Base):
	__tablename__ = 'channel'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True, nullable=False)

class Video(Base):
	__tablename__ = 'video'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True, nullable=False)
	num_frames = Column(Integer, nullable=False)
	fps = Column(Float, nullable=False)
	width = Column(Integer, nullable=False)
	height = Column(Integer, nullable=False)
	time = Column(DateTime, nullable=False)
	show_id = Column(Integer, ForeignKey('show.id'), nullable=False)
	is_duplicate = Column(Boolean, nullable=False)
	is_corrupt = Column(Boolean, nullable=False)

class FrameSampler(Base):
	__tablename__ = 'frame_sampler'
	id = Column(Integer, primary_key=True)
	name = Column(String, nullable=False)

class Frame(Base):
	__tablename__ = 'frame'
	id = Column(Integer, primary_key=True)
	number = Column(Integer, nullable=False)
	video_id = Column(Integer, ForeignKey('video.id'), nullable=False)
	sampler_id = Column(Integer, ForeignKey('frame_sampler.id'))

class Labeler(Base):
	__tablename__ = 'labeler'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True, nullable=False)
	created = Column(DateTime, server_default=func.now(), nullable=False)
	is_handlabel = Column(Boolean, nullable=False)
	comments = Column(String, nullable=True)

class Face(Base):
	__tablename__ = 'face'
	id = Column(Integer, primary_key=True)
	bbox_x1 = Column(Float, nullable=False)
	bbox_x2 = Column(Float, nullable=False)
	bbox_y1 = Column(Float, nullable=False)
	bbox_y2 = Column(Float, nullable=False)
	labeler_id = Column(Integer, ForeignKey('labeler.id'), nullable=False)
	score = Column(Float, nullable=False)
	frame_id = Column(Integer, ForeignKey('frame.id'), nullable=False)

class Identity(Base):
	__tablename__ = 'identity'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True)
	is_ignore = Column(Boolean, default=False, nullable=False)

class FaceIdentity(Base):
	__tablename__ = 'face_identity'
	face_id = Column(Integer, ForeignKey('face.id'), primary_key=True)
	labeler_id = Column(Integer, ForeignKey('labeler.id'), primary_key=True)
	score = Column(Float)
	identity_id = Column(Integer, ForeignKey('identity.id'), nullable=False)

class Gender(Base):
	__tablename__ = 'gender'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True, nullable=False)

class FaceGender(Base):
	__tablename__ = 'face_gender'
	face_id = Column(Integer, ForeignKey('face.id'), primary_key=True)
	gender_id = Column(Integer, ForeignKey('gender.id'), nullable=False)
	labeler_id = Column(Integer, ForeignKey('labeler.id'), primary_key=True)
	score = Column(Float)

class Commercial(Base):
	__tablename__ = 'commercial'
	id = Column(Integer, primary_key=True)
	labeler_id = Column(Integer, ForeignKey('labeler.id'), nullable=False)
	max_frame = Column(Integer, nullable=False)
	min_frame = Column(Integer, nullable=False)
	video_id = Column(Integer, ForeignKey('video.id'), nullable=False)

class ChannelHosts(Base):
	__tablename__ = 'channel_host'
	channel_id = Column(Integer, ForeignKey('channel.id'), primary_key=True)
	identity_id = Column(Integer, ForeignKey('identity.id'), primary_key=True)

class CanonicalShowHosts(Base):
	__tablename__ = 'canonical_show_host'
	canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'), primary_key=True)
	identity_id = Column(Integer, ForeignKey('identity.id'), primary_key=True)
