package com.github.karthik.kafka.tutorial3;


//import com.google.
import com.google.gson.JsonParser;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient(){

        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////

        // replace with your own credentials
        String hostname = "kafka-course-3327142882.eu-central-1.bonsaisearch.net"; // localhost or bonsai url
        String username = "c9kqybz93m"; // needed only for bonsai
        String password = "41pav3f1hs"; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

      public static KafkaConsumer<String, String> createConsumer(String twitter_tweets){
//
          String bootstrapServers = "127.0.0.1:9092";
          String groupId = "kafka-demo-elasticsearch";
          String topic = "twitter_tweets";
//
//        // create consumer configs
          Properties properties = new Properties();
          properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
          properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
          properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
          properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
          properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
          properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets
//
//        // create consumer
          KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
          consumer.subscribe(Arrays.asList(topic));
//
          return consumer;
//
     }
//
      private static JsonParser jsonParser = new JsonParser();
//
      private static String extractIdFromTweet(String tweetJson){
//        // gson library
          return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
       // Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        Logger logger = LoggerFactory.getLogger(Class.forName(ElasticSearchConsumer.class.getName()));
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
            Integer recordCount = records.count();
            logger.warn("Received " + records.count() + " records");
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records){
//                logger.warn("Key:" + record.key() + ", Value: " + record.value());
//                logger.warn("Partition: " + record.partition() + ", Offset:" + record.offset());
                // 2 strategies
                // kafka generic Id
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                // twitter feed specific id
                try {
                    String id = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest("twitter")
                            .source(record.value(), XContentType.JSON).id(id); // this is to make our consumer idempotent
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("skipping bad" + record.value());
                }
            }
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.warn("commiting offsets..");
                consumer.commitSync();
                logger.warn("Offsets have been commited");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        // close the client
        //client.close();
    }
}