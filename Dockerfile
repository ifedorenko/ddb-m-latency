# https://git.soma.salesforce.com/docker-images/sfdc_centos7_openjdk11
FROM dva-registry.internal.salesforce.com/dva/sfdc_centos7_onejdk11_basic

ADD target/ddb-m-latency-*.jar /app/

RUN useradd adhoc
USER adhoc
