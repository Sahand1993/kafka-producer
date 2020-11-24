FROM maven:3.6.3-jdk-11

ENV LINES_AT_A_TIME 1000
ENV SLEEP_DURATION 1000
ENV BOOTSTRAP_SERVERS 127.0.0.1:9092
ENV TOPIC_NAME measurements
ENV INPUT_FILE /data/input.csv

COPY pom.xml pom.xml
COPY src/ src/

RUN mvn clean install

CMD ["mvn", "exec:java", "-Dexec.mainClass=com.datareply.druid.MeasurementsProducer"]
