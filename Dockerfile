FROM sequenceiq/hadoop-docker
RUN yum install -y yum-plugin-ovl ; yum clean all
RUN yum install -y git ;  yum clean all
RUN yum update -y nss nss-util nspr 
RUN mkdir /root/output &&mkdir /root/code &&mkdir /root/input
COPY src /root/code
COPY run.sh /root/code
COPY README.md /root/code
COPY data-server /
COPY bootstrap.sh /etc/bootstrap.sh
EXPOSE 8102
