#!/bin/bash

# cd into catalog-harvesting directory to prevent python path errors
# setting PYTHONPATH does not seem to work
cd /opt/catalog-harvesting
exec setuser harvest gunicorn -b 0.0.0.0:3000 catalog_harvesting.api:app
