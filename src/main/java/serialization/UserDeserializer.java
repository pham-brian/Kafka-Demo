package serialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import model.User;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class UserDeserializer implements Deserializer<User> {
    private final Logger logger = LoggerFactory.getLogger(UserDeserializer.class);

    public void configure(Map<String, ?> map, boolean b) {

    }

    public User deserialize(String s, byte[] value) {
        logger.info("deserialize message: " + new String(value));
        if (null == value) {
            return null;
        }

        Gson gson = new GsonBuilder().create();
        User user = gson.fromJson(new String(value), User.class);
        logger.info("deserialize bidding:"+user);

        return user;
    }

    public void close() {

    }
}
