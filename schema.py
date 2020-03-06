from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CanonicalShow(Base):
	__tablename__ = 'canonical_show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	is_recurring = Column(Boolean)
	# TODO: unique constraint on name, channel
	# Add column: channel (some one-off videos like debates/speeches have the same name on all channels)

class Show(Base):
	__tablename__ = 'show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'))
	# TODO: unique constraint on name, channel
	# Add column: channel (some one-off videos like debates/speeches have the same name on all channels)

class Channel(Base):
	__tablename__ = 'channel'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	# TODO: unique constraint on name

class Video(Base):
	__tablename__ = 'video'
	id = Column(Integer, primary_key=True)
	path = Column(String)		# TODO: change to name
								# Make it so that it is only the file name component without .mp4
								# video_path.split('/')[-1].split('.mp4')[0]
	num_frames = Column(Integer)
	fps = Column(Float)
	width = Column(Integer)
	height = Column(Integer)
	time = Column(DateTime)
	channel_id = Column(Integer, ForeignKey('channel.id'))
	show_id = Column(Integer, ForeignKey('show.id'))
	has_captions = Column(Boolean)			# TODO: remove
	commercials_labeled = Column(Boolean)	# TODO: remove
	srt_extension = Column(String)			# TODO: remove
	threeyears_dataset = Column(Boolean)	# TODO: remove
	duplicate = Column(Boolean)
	corrupted = Column(Boolean)
	# TODO: unique constraint on name

class FrameSampler(Base):
	__tablename__ = 'frame_sampler'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class Frame(Base):
	__tablename__ = 'frame'
	id = Column(Integer, primary_key=True)
	number = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))
	shot_boundary = Column(Boolean)	# TODO: remove
	sampler_id = Column(Integer, ForeignKey('frame_sampler.id'))

class Labeler(Base):
	__tablename__ = 'labeler'
	id = Column(Integer, primary_key=True)
	name = Column(String, unique=True)
	created = Column(DateTime)
	data_path = Column(String)		# TODO: rename to comments
	# unique constraint on name

class Face(Base):
	__tablename__ = 'face'
	id = Column(Integer, primary_key=True)
	bbox_x1 = Column(Float)
	bbox_x2 = Column(Float)
	bbox_y1 = Column(Float)
	bbox_y2 = Column(Float)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	shot_id = Column(Integer)		# TODO: remove
	background = Column(Boolean)	# TODO: remove
	is_host = Column(Boolean)		# TODO: remove
	blurriness = Column(Float)		# TODO: remove
	probability = Column(Float)		# TODO: rename to score
	frame_id = Column(Integer, ForeignKey('frame.id'))

class Identity(Base):
	__tablename__ = 'identity'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	# TODO: unique constraint on name
	# 		add boolean is_ignore

class FaceIdentity(Base):
	__tablename__ = 'face_identity'
	id = Column(Integer, primary_key=True)
	face_id = Column(Integer, ForeignKey('face.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	probability = Column(Float)		# TODO: rename to score
	identity_id = Column(Integer, ForeignKey('identity.id'))
	# TODO: unique constraint on face_id and labeler_id

class Gender(Base):
	__tablename__ = 'gender'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	# TODO: unique constraint on name

class FaceGender(Base):
	__tablename__ = 'face_gender'
	id = Column(Integer, primary_key=True)
	face_id = Column(Integer, ForeignKey('face.id'))
	gender_id = Column(Integer, ForeignKey('gender.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	probability = Column(Float)		# TODO: rename to score
	# TODO: unique constraint on face_id and labeler_id

class Commercial(Base):
	__tablename__ = 'commercial'
	id = Column(Integer, primary_key=True)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	max_frame = Column(Integer)
	min_frame = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))

class ShowHosts(Base):
	__tablename__ = 'show_hosts'
	id = Column(Integer, primary_key=True)
	show_id = Column(Integer, ForeignKey('show.id'))
	identity_id = Column(Integer, ForeignKey('identity.id'))

class CanonicalShowHosts(Base):
	__tablename__ = 'canonical_show_hosts'
	id = Column(Integer, primary_key=True)
	canonicalshow_id = Column(Integer, ForeignKey('canonical_show.id'))
	identity_id = Column(Integer, ForeignKey('identity.id'))

# TODO:
# It would be better to merge ShowHosts and CanonicalShowHosts
# Really, what we want is that hosts get attributed to channels, and sometimes
# to shows
#
# I propose:
# class HostsAndStaff(Base):
# 	id = Column(Integer, primary_key=True)
# 	channel_id 			<-- nullable
# 	canonicalshow_id	<-- nullable
# 	identity_id
