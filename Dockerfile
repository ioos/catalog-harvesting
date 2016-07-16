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
RUN pip install -e git+https://github.com/ioos/catalog-harvesting.git@c6c0e83bc0c92ceb01bccd26a8c654f9917f75fc#egg=catalog-harvesting
RUN useradd -m harvest
RUN mkdir /var/log/harvest
RUN chown harvest:harvest /var/log/harvest
COPY ./contrib/my_init.d /etc/my_init.d
VOLUME ["/data"]

CMD ["/sbin/my_init"]

