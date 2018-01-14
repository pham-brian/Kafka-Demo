package kafka.producer;

import kafka.KafkaProperties;
import model.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class UserProducer {

    private static Properties createProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.KAFKA_BROKERS);
        props.put("client.id", "KafkaExampleProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "serialization.UserSerializer");
        return props;
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        KafkaProducer<Integer, User> producer = new KafkaProducer<Integer, User>(createProps());
        long time = System.currentTimeMillis();
        try {
            int i = 1;
            for (long index = time; index < time + sendMessageCount; index++) {
                User user =  new User("Test", "test", i);
                final ProducerRecord<Integer, User> record =
                        new ProducerRecord<Integer, User>(KafkaProperties.MY_TOPIC, user);
                i++;
                System.out.println("Sent User " + user.toString());
                producer.send(record);
            }
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            runProducer(20);
        } else {
            runProducer(Integer.parseInt(args[0]));
        }
    }
}
