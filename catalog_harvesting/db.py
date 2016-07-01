#!/bin/bash
'''
catalog_harvesting/db.py
'''

from sqlalchemy import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


def initdb(engine):
    metadata = MetaData(bind=engine, schema='catalog_registry')

    exports = {}

    class CatalogHarvests(Base):
        __table__ = Table('catalog_harvests', metadata, autoload=True)

    exports['CatalogHarvests'] = CatalogHarvests
    return exports

