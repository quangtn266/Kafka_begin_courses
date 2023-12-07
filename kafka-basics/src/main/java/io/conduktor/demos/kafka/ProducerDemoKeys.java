package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//public class ProducerDemo {
public class ProducerDemoKeys {
    //private  static  final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    private  static  final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I'm a Kafka producer");

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // connect to Conduktor playground
        //properties.setProperty("bootstrap.servers", "redpanda-0:9092");
        //properties.setProperty("security.protocol", "SASL_SSL");
        //properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='quangtn933@gmail.com' password='Gh0st1@3$';");
        //properties.setProperty("sasl.mechanism", "SASL_SSL");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j<2; j++ ) {

            for (int i = 0; i < 30; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world" + i;

                // create a Producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);
                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // execute every time a record sucessfully sent or an exeption is thrown
                        if (e == null) {
                            // the record was sucessfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing");
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

            // flush and close the produce}

        // tell the producer to send all data and block until done
        producer.flush();

        // flush and close the producer
        producer.close();

    }

}
