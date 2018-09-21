FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install -y python-pip python-dev \
    && cd /usr/local/bin \
    && ln -s /usr/bin/python python \
    && pip install --upgrade pip \
    && apt-get install -y  software-properties-common \
    && add-apt-repository ppa:webupd8team/java -y \
    && echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
    && apt-get update \
    && apt-get install -y oracle-java8-installer \
    && apt-get clean

# RUN apt-get update \
#     && apt-get install -y python3-pip python3-dev \
#     && cd /usr/local/bin \
#     && ln -s /usr/bin/python3 python \
#     && pip3 install --upgrade pip \
#     && apt-get install -y  software-properties-common \
#     && add-apt-repository ppa:webupd8team/java -y \
#     && echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
#     && apt-get update \
#     && apt-get install -y oracle-java8-installer \
#     && apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
#ENV SPARK_MASTER spark://ymslanda.innovationgarage.tech:7077
#ENV SPARK_DRIVER_HOST ymslanda.innovationgarage.tech

# Set the working directory to /AISroot
WORKDIR /AISroot

# Copy the current directory contents into the container at /AISroot
ADD . /AISroot

RUN pip install -r ./requirements.txt

# RUN pip3 install -r ./requirements.txt

ADD reduceAIS.sh /reduceAIS.sh
CMD ["/reduceAIS.sh"]