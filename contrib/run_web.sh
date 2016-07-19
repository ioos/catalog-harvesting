#!/bin/bash

exec setuser harvest gunicorn -b 0.0.0.0:3000 catalog_harvesting.api:app
