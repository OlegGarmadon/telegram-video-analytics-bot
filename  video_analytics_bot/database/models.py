from sqlalchemy import Column, BigInteger, Integer, DateTime, ForeignKey, func
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
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Связь один-ко-многим
    snapshots = relationship("VideoSnapshot", back_populates="video")

class VideoSnapshot(Base):
    __tablename__ = 'video_snapshots'
    
    id = Column(BigInteger, primary_key=True)
    video_id = Column(BigInteger, ForeignKey('videos.id'), nullable=False)
    views_count = Column(Integer, nullable=False, default=0)
    likes_count = Column(Integer, nullable=False, default=0)
    comments_count = Column(Integer, nullable=False, default=0)
    reports_count = Column(Integer, nullable=False, default=0)
    delta_views_count = Column(Integer, nullable=False, default=0)
    delta_likes_count = Column(Integer, nullable=False, default=0)
    delta_comments_count = Column(Integer, nullable=False, default=0)
    delta_reports_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Связь многие-к-одному
    video = relationship("Video", back_populates="snapshots")