from sqlalchemy import *
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CanonicalShow(Base):
    __tablename__ = 'canonical_show'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    is_recurring = Column(Boolean, server_default='f')
    channel_id = Column(Integer, ForeignKey('channel.id'), nullable=False)

    UniqueConstraint('unique_name_channel', name, channel_id, deferrable=True)

class Show(Base):
    __tablename__ = 'show'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'), nullable=False, index=True)
    channel_id = Column(Integer, ForeignKey('channel.id'), nullable=False)

    UniqueConstraint('unique_name_channel', name, channel_id, deferrable=True)

class Channel(Base):
    __tablename__ = 'channel'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

class Video(Base):
    __tablename__ = 'video'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False, index=True)
    extension = Column(String, nullable=False)
    num_frames = Column(Integer, nullable=False)
    fps = Column(Float, nullable=False)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    time = Column(DateTime, nullable=False, index=True)
    show_id = Column(Integer, ForeignKey('show.id'), nullable=False, index=True)
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
    video_id = Column(Integer, ForeignKey('video.id'), nullable=False, index=True)
    sampler_id = Column(Integer, ForeignKey('frame_sampler.id'))

class Labeler(Base):
    __tablename__ = 'labeler'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False, index=True)
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
    frame_id = Column(Integer, ForeignKey('frame.id'), nullable=False, index=True)

class Identity(Base):
    __tablename__ = 'identity'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, index=True)
    is_ignore = Column(Boolean, server_default='f', nullable=False)

class FaceIdentity(Base):
    __tablename__ = 'face_identity'
    face_id = Column(Integer, ForeignKey('face.id'), primary_key=True)
    labeler_id = Column(Integer, ForeignKey('labeler.id'), primary_key=True)
    score = Column(Float)
    identity_id = Column(Integer, ForeignKey('identity.id'), nullable=False, index=True)

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
    video_id = Column(Integer, ForeignKey('video.id'), nullable=False, index=True)

class ChannelHosts(Base):
    __tablename__ = 'channel_host'
    channel_id = Column(Integer, ForeignKey('channel.id'), primary_key=True)
    identity_id = Column(Integer, ForeignKey('identity.id'), primary_key=True)

class CanonicalShowHosts(Base):
    __tablename__ = 'canonical_show_host'
    canonical_show_id = Column(Integer, ForeignKey('canonical_show.id'), primary_key=True)
    identity_id = Column(Integer, ForeignKey('identity.id'), primary_key=True)
