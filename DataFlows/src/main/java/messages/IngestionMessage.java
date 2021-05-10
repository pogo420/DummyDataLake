package messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IngestionMessage {
    /** Class for Managing Ingestion Message */

    public static final String PROCESSING_TIME = "_pt";
    public static final String PAYLOAD = "_pl";

    @JsonProperty(PAYLOAD)
    private ObjectNode payload;

    @JsonCreator
    IngestionMessage(@JsonProperty(PAYLOAD) ObjectNode payload) {
       this.payload = payload;
   }

    public ObjectNode getPayload() {
        // return the payload
        return payload;
    }

    public String messageSerial() {
        // returns serialized message
        return Json.serialize(this);
    }

    public static IngestionMessage messageDeSerial(String json) {
        // returns de serialized message
        return Json.deserialize(json, IngestionMessage.class);
    }

    public static IngestionMessage setPayload(ObjectNode node) {
        // creating new payload
        return new IngestionMessage(node);
    }

}
