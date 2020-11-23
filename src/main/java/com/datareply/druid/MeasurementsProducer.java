package com.datareply.druid;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import com.google.gson.Gson;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MeasurementsProducer {

    private static final String TOPIC = System.getenv("TOPIC_NAME");
    private static final String BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
    private static final String CLIENT_ID = "MeasurementsProducer";
    private static final String INPUT_FILE = System.getenv("INPUT_FILE");
    private static final int LINES_AT_A_TIME = Integer.parseInt(System.getenv("LINES_AT_A_TIME"));
    private static final long SLEEP_DURATION = Long.parseLong(System.getenv("SLEEP_DURATION"));

    static Gson gson = new Gson();

    private static final Logger LOG = LoggerFactory.getLogger(MeasurementsProducer.class);

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        runProducer();
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    static void runProducer() throws IOException, InterruptedException, ExecutionException {
        
        final Producer<String, String> producer = createProducer();
        while (!Thread.currentThread().isInterrupted()) {
            try (BufferedReader inputReader = new BufferedReader(new FileReader(new File(INPUT_FILE)))) {
                String nextLine;
                int recordsSent = 0;
                while ((nextLine = inputReader.readLine()) != null) {
                    Measurement measurement = fromCsv(nextLine);
                    if (measurement.isBool()) {
                        measurement.setTimestamp(System.currentTimeMillis());

                        producer.send(createProducerRecord(measurement));
                        
                        if (recordsSent % LINES_AT_A_TIME == 0) {
                            LOG.info("Sent {} measurements. Going to sleep for {} milliseconds.", recordsSent, SLEEP_DURATION);
                            Thread.sleep(SLEEP_DURATION);
                        }
                        recordsSent++;    
                    }
                }
            } 

            LOG.info("Sent the whole file. Starting over...");
        }
    }

    private static ProducerRecord<String, String> createProducerRecord(Measurement measurement) {
        return new ProducerRecord<>(
            TOPIC,
            String.valueOf(measurement.getHouseId()), 
            gson.toJson(measurement)
        );
    }

    private static Measurement fromCsv(String nextLine) {
        Measurement measurement = new Measurement();
        String[] values = nextLine.split(",");
        measurement.setValue(Double.parseDouble(values[2]));
        measurement.setBool("1".equals(values[3]));
        measurement.setPlugId(Integer.parseInt(values[4]));
        measurement.setRoomId(Integer.parseInt(values[5]));
        measurement.setHouseId(Integer.parseInt(values[6]));
        measurement.setTimestamp(System.currentTimeMillis());
        return measurement;
    }
}
