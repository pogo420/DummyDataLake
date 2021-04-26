package messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IngestionMessage {

    @JsonProperty("_pl")
    private ObjectNode payload;

    @JsonCreator
    IngestionMessage(@JsonProperty("_pl") ObjectNode payload) {
       this.payload = payload;
   }

    public ObjectNode getPayload() {
        return payload;
    }

    public String messageSerial() {
        return Json.serialize(this);
    }

    public static IngestionMessage messageDeSerial(String json) {
        return Json.deserialize(json, IngestionMessage.class);
    }

}
