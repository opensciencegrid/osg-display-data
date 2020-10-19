FROM ubuntu:20.04

WORKDIR /osg-display-data

# Install dependencies
RUN apt-get update -qq && \
  echo ttf-mscorefonts-installer msttcorefonts/accepted-mscorefonts-eula select true | debconf-set-selections && \
  DEBIAN_FRONTEND=noninteractive apt-get install -y \
    msttcorefonts \
    python3-matplotlib \
    python3-pip \
  && apt-get -y autoclean && \
  apt-get -y autoremove && \
  rm -rf /var/lib/apt/lists/*

# Install application dependencies
COPY requirements.txt setup.py ./
RUN pip3 install --no-cache-dir -r requirements.txt

# Add config
COPY config ./config

# Install application and add cron entry
COPY src ./src
RUN python3 setup.py install && \
    mkdir -p /var/www/html/osg_display

CMD ["/usr/bin/osg_display", "--daemon", "900", "-q"]
