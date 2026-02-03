from sqlalchemy import Column, BigInteger, Integer, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Video(Base):
    __tablename__ = 'videos'
    
    id = Column(BigInteger, primary_key=True)
    creator_id = Column(BigInteger, nullable=False)
    video_created_at = Column(DateTime, nullable=False)
    views_count = Column(Integer, nullable=False, default=0)
    likes_count = Column(Integer, nullable=False, default=0)
    comments_count = Column(Integer, nullable=False, default=0)
    reports_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Связь один-ко-многим с видео снапшотами
    snapshots = relationship("VideoSnapshot", back_populates="video", cascade="all, delete-orphan")

class VideoSnapshot(Base):
    __tablename__ = 'video_snapshots'
    
    id = Column(BigInteger, primary_key=True)
    video_id = Column(BigInteger, ForeignKey('videos.id', ondelete='CASCADE'), nullable=False)
    views_count = Column(Integer, nullable=False, default=0)
    likes_count = Column(Integer, nullable=False, default=0)
    comments_count = Column(Integer, nullable=False, default=0)
    reports_count = Column(Integer, nullable=False, default=0)
    delta_views_count = Column(Integer, nullable=False, default=0)
    delta_likes_count = Column(Integer, nullable=False, default=0)
    delta_comments_count = Column(Integer, nullable=False, default=0)
    delta_reports_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Связь многие-к-одному с видео
    video = relationship("Video", back_populates="snapshots")