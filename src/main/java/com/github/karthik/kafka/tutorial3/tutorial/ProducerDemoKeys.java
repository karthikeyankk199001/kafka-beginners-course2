package com.github.karthik.kafka.tutorial3.tutorial;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServers = "127.0.0.1:9092";
        // create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i =0 ; i < 10 ; i++) {
            // create a producer record
            String topic = "first_topic_java";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" +  Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key,value);

            logger.warn("Key: " + key); //log the key
            // send data - Async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute every time an record is successfully sent or an exception thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.warn("Received new metadata: \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block the send() to make it sync , Not for PROD
        }
        //flush data
        producer.flush();

        //flush and close data
        producer.close();

    }
}
