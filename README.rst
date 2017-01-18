Catalog Harvesting
==================

Python Modules to synchronize third party metadata sources with a central
metadata repository (WAF)

Installing
----------------

This project suppliments the IOOS Registry by providing the ability to harvest
from WAFs, store the documents in a central WAF and update the MongoDB database
that the registry uses.  You'll need a running redis service to run this
project. MongoDB is the means by which this project's workers can communicate
job status with the main registry project.

This project supports only Python 2.7 due to the CKAN dependency.

Under a python virtual environment or the system python::

    # Install the project dependencies
    pip install -r requirements_ext.txt
    pip install -r requirements.txt
    pip install gunicorn

    # Install this project
    pip install -e .


These commands will install the project dependencies and install the project to
the either the current python virtual environment or to the system path for
Python.

Configuring the project
-----------------------

There is an environment variable ``OUTPUT_DIR`` that defines where the
documents should be written to from source WAFs.


Usage
-----

To run the project::

    gunicorn -b localhost:3000 catalog_harvesting.api:app --workers 4

To manually execute a one-time harvest::

    catalog-harvest -s <MongoDB URL> -d <WAF Directory> -v

Docker
------

Building
^^^^^^^^

To build the project, is pretty simple::

    docker build -t ioos/catalog-harvesting .

The project is also automatically built by dockerhub whenever a change is made
to the master branch, usually through pull-requests.

