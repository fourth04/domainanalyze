# coding: utf-8
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
metadata = Base.metadata

def to_dict(self):
    return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}

Base.to_dict = to_dict

class Customer(Base):
    __tablename__ = 'customer'

    id = Column(Integer, primary_key=True)
    name = Column(String(45, 'utf8_unicode_ci'), nullable=False)
    note = Column(String(128, 'utf8_unicode_ci'))
    add_time = Column(DateTime)


class UrlResult(Base):
    __tablename__ = 'url_result'

    id = Column(Integer, primary_key=True)
    status = Column(String(45, 'utf8_unicode_ci'), index=True)
    dname = Column(String(255, 'utf8_unicode_ci'), nullable=False, index=True)
    category = Column(String(255, 'utf8_unicode_ci'))
    tencent_info = Column(String(1024, 'utf8_unicode_ci'))
    icp_info = Column(String(1024, 'utf8_unicode_ci'))
    dns_provider = Column(String(1024, 'utf8_unicode_ci'))
    whois_info = Column(String(3072, 'utf8_unicode_ci'))
    dns_info = Column(String(2048, 'utf8_unicode_ci'))
    add_time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, index=True)


class UrlTask(Base):
    __tablename__ = 'url_task'

    id = Column(Integer, primary_key=True)
    status = Column(String(45, 'utf8_unicode_ci'), nullable=False, index=True)
    dname = Column(String(255, 'utf8_unicode_ci'), nullable=False, index=True)
    addresses = Column(String(1024, 'utf8_unicode_ci'))
    add_time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, index=True)
    customer_id = Column(ForeignKey('customer.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False, index=True)
    url_result_id = Column(ForeignKey('url_result.id', ondelete='CASCADE', onupdate='CASCADE'), index=True)

    customer = relationship('Customer')
    url_result = relationship('UrlResult')

class RegisterResult(Base):
    __tablename__ = 'register_result'

    id = Column(Integer, primary_key=True)
    status = Column(String(45, 'utf8_unicode_ci'), nullable=False, index=True)
    domain = Column(String(255, 'utf8_unicode_ci'), nullable=False, index=True)
    register_status = Column(String(45, 'utf8_unicode_ci'), nullable=False, index=True)
    result = Column(String(1024, 'utf8_unicode_ci'), nullable=False)
    add_time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, nullable=False, index=True)


class RegisterTask(Base):
    __tablename__ = 'register_task'

    id = Column(Integer, primary_key=True)
    status = Column(String(45, 'utf8_unicode_ci'), index=True)
    domain = Column(String(255, 'utf8_unicode_ci'), nullable=False)
    addresses = Column(String(1024, 'utf8_unicode_ci'))
    add_time = Column(DateTime, nullable=False)
    update_time = Column(DateTime, index=True)
    register_result_id = Column(ForeignKey('register_result.id', ondelete='CASCADE', onupdate='CASCADE'), index=True)

    register_result = relationship('RegisterResult')
