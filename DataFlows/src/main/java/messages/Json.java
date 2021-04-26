package messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Json {

    /** Class for managing json processing */

    private Json(){
        ;
    }

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ObjectMapper getMapper(){
        /**static method to get object mapper */

        return objectMapper;
    }

    public static String serialize(Object node) {
        /** static method for serialization: Class to String */
        try {
            return getMapper().writeValueAsString(node);
        }
        catch (JsonProcessingException e){
            throw new RuntimeException(e);
        }

    }

    public static <T> T deserialize(String string, Class<T> class_) {
        /** static method for deserialization: String to Class */
        try {
            return getMapper().readValue(string, class_);
        }
        catch (JsonProcessingException e){
            throw new RuntimeException(e);
        }

    }

}
