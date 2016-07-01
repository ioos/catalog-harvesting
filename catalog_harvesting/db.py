#!/bin/bash
'''
catalog_harvesting/db.py
'''

from sqlalchemy import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


def initdb(engine):
    metadata = MetaData(bind=engine, schema='catalog_registry')

    exports = {}

    class Organizations(Base):
        __table__ = Table('organizations', metadata, autoload=True)

    class CatalogHarvests(Base):
        __table__ = Table('catalog_harvests', metadata, autoload=True)
        organization = relationship(Organizations)

    exports['CatalogHarvests'] = CatalogHarvests
    return exports

