FROM apache/spark:4.0.4
USER root
COPY *.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar
USER spark
