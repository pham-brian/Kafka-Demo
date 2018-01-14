package serialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import model.User;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class UserSerializer implements Serializer<User> {
    private final Logger logger = LoggerFactory.getLogger(UserSerializer.class);

    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, User user) {
        if (null == user) {
            return null;
        }
        Gson gson = new GsonBuilder().create();

        return gson.toJson(user).getBytes();
    }

    public void close() {
    }
}
