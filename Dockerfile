FROM sequenceiq/hadoop-docker
RUN yum install -y yum-plugin-ovl ; yum clean all
RUN yum install -y git ;  yum clean all
RUN yum update -y nss nss-util nspr 
COPY data-server /
COPY bootstrap.sh /etc/bootstrap.sh
EXPOSE 8102
