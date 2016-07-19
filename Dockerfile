FROM phusion/baseimage:0.9.18

MAINTAINER Luke Campbell <luke.campbell@rpsgroup.com>

RUN apt-get update && apt-get install -y \
      git \
      libssl-dev \
      libxml2-dev \
      libxslt1-dev \
      libpq-dev \
      python-dev \
      python-pip
RUN rm -rf /var/lib/apt/lists/*
RUN pip install -U pip
RUN mkdir /opt/catalog-harvesting
COPY catalog_harvesting /opt/catalog-harvesting/catalog_harvesting
COPY setup.py README.rst requirements.txt LICENSE /opt/catalog-harvesting/
RUN pip install -e /opt/catalog-harvesting
RUN pip install gunicorn
RUN useradd -m harvest
RUN mkdir /var/log/harvest
RUN chown harvest:harvest /var/log/harvest
COPY ./contrib/my_init.d /etc/my_init.d
COPY ./contrib/run_web.sh /
VOLUME ["/data"]
ENV OUTPUT_DIR /data
EXPOSE 3000

CMD ["/sbin/my_init", "--", "/run_web.sh"]

