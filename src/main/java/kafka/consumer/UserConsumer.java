package kafka.consumer;

import kafka.KafkaProperties;
import model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class UserConsumer {

    private static Properties createProps() {
        final Properties props = new Properties();
        props.put("bootstrap.servers",
                KafkaProperties.KAFKA_BROKERS);
        props.put("group.id",
                "KafkaExampleConsumer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "serialization.UserDeserializer");
        return props;
    }

    static void runConsumer() throws InterruptedException {
        KafkaConsumer<Integer, User> consumer= new KafkaConsumer<Integer, User>(createProps());
        consumer.subscribe(Collections.singletonList(KafkaProperties.MY_TOPIC));
        while (true) {
            ConsumerRecords<Integer, User> messages = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<Integer, User> message : messages) {
                System.out.println("Message received " + message.value().toString());
            }
            consumer.commitAsync();
        }
    }

    public static void main(String[] args) throws Exception {
        runConsumer();
    }
}
