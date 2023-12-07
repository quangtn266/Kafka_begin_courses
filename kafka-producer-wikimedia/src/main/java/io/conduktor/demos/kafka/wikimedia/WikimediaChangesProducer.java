package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        //String boostrapServers = "localhost:9092";
        //String groupId = "wikimedia-change-producer";

        // Create Producer properties.
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", boostrapServers);

        // connect to Conduktor playground
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // 127.0.0.1:9092
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty("sasl.mechanism", "SASL_SSL");

        // set safe producer configs (Kafka <= 2.8)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // set high through producer configs
        properties.setProperty(ProducerConfig. LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig. BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig. COMPRESSION_TYPE_CONFIG, "snappy");

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();


        // Start the producer in another thread
        eventSource.start();

        // we produce for 10 mins and block the program until then

        TimeUnit.MINUTES.sleep(10);

    }
}
