package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Slf4j
public class TwitterProducer {
    public static void main(String[] args) {
        log.info("Running Twitter producer");
        new TwitterProducer().run();
    }

    public void run() {
        log.info("Setup");

        // Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection
        client.connect();

        // kafka producer
        KafkaProducer<String, String> producer = createKafkaProducers();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            log.info("Shutting down client from twitter...");
            client.stop();
            log.info("Closing producer...");
            producer.close();
            log.info("done!");
        }));

        //  loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Error polling messages: {}", e.getMessage());
                client.stop();
            }
            if(Objects.nonNull(msg)) {
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
                    if (Objects.nonNull(e)) {
                        log.error("Error publishing data: {}", e.getMessage());
                    }
                });
            }
        }
        log.info("End of application");

    }

    private KafkaProducer<String, String> createKafkaProducers() {
        String bootstrapServer = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise

        // high throughput producer (at the expense of bit latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB batch size

        // Create producer
        return new KafkaProducer<>(properties);
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        String consumerKey = "8EmSyaiHByLCl7uyrdYextK1V";
        String consumerSecret = "pxMnoSidMwAlMt5bEkFmeqN6HZyvbVe3xr94kNVsFztzgxJKzN";
        String token = "1140776539086344192-AfqmRtXhvTRdgddWExOvDbfNS33QQO";
        String secret = "SACS3SMQUswx5ut8NSMtjRSZuKJuuynXiDZdIRHJ0M2Tq";
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }
}
