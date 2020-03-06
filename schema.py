from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class LabeledPanel(Base):
	__tablename__ = 'labeled_panel'
	id = Column(Integer, primary_key=True)
	start = Column(Integer)
	end = Column(Integer)
	num_panelists = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))

class Gender(Base):
	__tablename__ = 'gender'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class HairLength(Base):
	__tablename__ = 'hair_length'
	id = Column(Integer, primary_key=True)
	face_id = Column(Integer, ForeignKey('face.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	length_id = Column(Integer, ForeignKey('hair_length_name.id'))

class Clothing(Base):
	__tablename__ = 'clothing'
	id = Column(Integer, primary_key=True)
	clothing_id = Column(Integer, ForeignKey('clothing.id'))
	face_id = Column(Integer, ForeignKey('face.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))

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

class ScannerJob(Base):
	__tablename__ = 'scanner_job'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class FrameSampler(Base):
	__tablename__ = 'frame_sampler'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class Show(Base):
	__tablename__ = 'show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'))

class Tag(Base):
	__tablename__ = 'tag'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class FaceTag(Base):
	__tablename__ = 'face_tag'
	id = Column(Integer, primary_key=True)
	score = Column(Float)
	face_id = Column(Integer, ForeignKey('face.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	tag_id = Column(Integer, ForeignKey('tag.id'))

class Identity(Base):
	__tablename__ = 'identity'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class ClothingName(Base):
	__tablename__ = 'clothing_name'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class HairColor(Base):
	__tablename__ = 'hair_color'
	id = Column(Integer, primary_key=True)
	color_id = Column(Integer, ForeignKey('hair_color_name.id'))
	face_id = Column(Integer, ForeignKey('face.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))

class IdentityTag(Base):
	__tablename__ = 'identity_tag'
	id = Column(Integer, primary_key=True)

class IdentityTags(Base):
	__tablename__ = 'identity_tags'
	id = Column(Integer, primary_key=True)

class Face(Base):
	__tablename__ = 'face'
	id = Column(Integer, primary_key=True)
	bbox_x1 = Column(Float)
	bbox_x2 = Column(Float)
	bbox_y1 = Column(Float)
	bbox_y2 = Column(Float)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	shot_id = Column(Integer, ForeignKey('shot.id'))
	background = Column(Boolean)
	is_host = Column(Boolean)
	blurriness = Column(Float)
	probability = Column(Integer)
	frame_id = Column(Integer, ForeignKey('frame.id'))

class SegmentTopics(Base):
	__tablename__ = 'segment_topics'
	id = Column(Integer, primary_key=True)
	segment_id = Column(Integer, ForeignKey('segment.id'))
	topic_id = Column(Integer, ForeignKey('topic.id'))

class Commercial(Base):
	__tablename__ = 'commercial'
	id = Column(Integer, primary_key=True)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	max_frame = Column(Integer)
	min_frame = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))

class Channel(Base):
	__tablename__ = 'channel'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class FaceFeatures(Base):
	__tablename__ = 'face_features'
	id = Column(Integer, primary_key=True)

class Shot(Base):
	__tablename__ = 'shot'
	id = Column(Integer, primary_key=True)
	min_frame = Column(Integer)
	max_frame = Column(Integer)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	video_id = Column(Integer, ForeignKey('video.id'))
	in_commercial = Column(Boolean)

class ShowHosts(Base):
	__tablename__ = 'show_hosts'
	id = Column(Integer, primary_key=True)
	show_id = Column(Integer, ForeignKey('show.id'))
	identity_id = Column(Integer, ForeignKey('identity.id'))

class HairLengthName(Base):
	__tablename__ = 'hair_length_name'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class VideoTag(Base):
	__tablename__ = 'video_tag'
	id = Column(Integer, primary_key=True)
	tag_id = Column(Integer, ForeignKey('tag.id'))
	video_id = Column(Integer, ForeignKey('video.id'))

class Topic(Base):
	__tablename__ = 'topic'
	id = Column(Integer, primary_key=True)
	name = Column(String)

class CanonicalShow(Base):
	__tablename__ = 'canonical_show'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	is_recurring = Column(Boolean)

class FaceIdentity(Base):
	__tablename__ = 'face_identity'
	id = Column(Integer, primary_key=True)
	face_id = Column(Integer, ForeignKey('face.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	probability = Column(Integer)
	identity_id = Column(Integer, ForeignKey('identity.id'))

class Object(Base):
	__tablename__ = 'object'
	id = Column(Integer, primary_key=True)
	bbox_x1 = Column(Float)
	bbox_x2 = Column(Float)
	bbox_y1 = Column(Float)
	bbox_y2 = Column(Float)
	label = Column(Integer)
	probability = Column(Float)
	frame_id = Column(Integer, ForeignKey('frame.id'))

class FaceGender(Base):
	__tablename__ = 'face_gender'
	id = Column(Integer, primary_key=True)
	face_id = Column(Integer, ForeignKey('face.id'))
	gender_id = Column(Integer, ForeignKey('gender.id'))
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	probability = Column(Integer)

class Pose(Base):
	__tablename__ = 'pose'
	id = Column(Integer, primary_key=True)

class LabeledCommercial(Base):
	__tablename__ = 'labeled_commercial'
	id = Column(Integer, primary_key=True)
	start = Column(Integer)
	end = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))

class Segment(Base):
	__tablename__ = 'segment'
	id = Column(Integer, primary_key=True)
	min_frame = Column(Integer)
	max_frame = Column(Integer)
	labeler_id = Column(Integer, ForeignKey('labeler.id'))
	video_id = Column(Integer, ForeignKey('video.id'))
	polarity = Column(Float)
	subjectivity = Column(Float)

class FrameTags(Base):
	__tablename__ = 'frame_tags'
	id = Column(Integer, primary_key=True)
	frame_id = Column(Integer, ForeignKey('frame.id'))
	tag_id = Column(Integer, ForeignKey('tag.id'))

class Frame(Base):
	__tablename__ = 'frame'
	id = Column(Integer, primary_key=True)
	number = Column(Integer)
	video_id = Column(Integer, ForeignKey('video.id'))
	shot_boundary = Column(Boolean)
	sampler_id = Column(Integer, ForeignKey('frame_sampler.id'))

class LabeledInterview(Base):
	__tablename__ = 'labeled_interview'
	id = Column(Integer, primary_key=True)
	start = Column(Integer)
	end = Column(Integer)
	interviewer1 = Column(String)
	interviewer2 = Column(String)
	guest1 = Column(String)
	guest2 = Column(String)
	original = Column(Boolean)
	scattered_clips = Column(Boolean)
	video_id = Column(Integer, ForeignKey('video.id'))

class CanonicalShowHosts(Base):
	__tablename__ = 'canonical_show_hosts'
	id = Column(Integer, primary_key=True)
	canonicalshow_id = Column(Integer, ForeignKey('canonical_show.id'))
	identity_id = Column(Integer, ForeignKey('identity.id'))

class Labeler(Base):
	__tablename__ = 'labeler'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	created = Column(DateTime)
	data_path = Column(String)

class HairColorName(Base):
	__tablename__ = 'hair_color_name'
	id = Column(Integer, primary_key=True)
	name = Column(String)
