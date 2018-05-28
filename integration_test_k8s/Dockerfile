FROM centos

RUN yum install -y \
       java-1.8.0-openjdk \
       java-1.8.0-openjdk-devel

ENV JAVA_HOME /etc/alternatives/jre

RUN bash -c "$(/bin/echo -e "cat >> /etc/yum.repos.d/google-cloud-sdk.repo <<EOM \
\n[google-cloud-sdk] \
\nname=Google Cloud SDK \
\nbaseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el7-x86_64 \
\nenabled=1 \
\ngpgcheck=1 \
\nrepo_gpgcheck=1 \
\ngpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg \
\n       https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg \
\nEOM\n")"; \
        yum update -y && \
        yum install -y \
            google-cloud-sdk \
            kubectl \
            which

RUN bash -c "$(/bin/echo -e "cat >> /etc/yum.repos.d/epel-apache-maven.repo <<EOM \
\n[epel-apache-maven] \
\nname=maven from apache foundation. \
\nbaseurl=http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-7Server/x86_64/ \
\nenabled=1 \
\nskip_if_unavailable=1 \
\ngpgcheck=0 \
\nEOM\n")"; \
        yum install -y \
            apache-maven

COPY spydra /spydra

COPY bootstrap.sh /bootstrap.sh
ENTRYPOINT ["/bootstrap.sh"]
