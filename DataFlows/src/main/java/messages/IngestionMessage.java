package messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import schema_loader.SchemaLoader;

import java.io.Serializable;

public class IngestionMessage implements Serializable {
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

    public static Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>> validator(MessageWrapper<IngestionMessage> ingestionMessageWrapped, SchemaLoader schemaLoader) {

        int col = 0;
        IngestionMessage ingestionMessage = ingestionMessageWrapped.getMessageObj();

        while(col < schemaLoader.getNumColumns()) {

            String columnName = schemaLoader.getColumnNameByIndex(col);
            String columnDataType = schemaLoader.getSchemaColumnDataTypeKey(col);

            // missing column case
            if (!ingestionMessage.getPayload().has(columnName)) {
                ingestionMessageWrapped.setFailureFlag(true);
                ingestionMessageWrapped.setFailureMessage(String.format("key %s is missing in payload", columnName));
                return Tuple.of(ingestionMessage, ingestionMessageWrapped);
            }
            col++;
        }

        // success case
        ingestionMessageWrapped.setFailureFlag(false);
        ingestionMessageWrapped.setFailureMessage(null);
        return Tuple.of(ingestionMessage, ingestionMessageWrapped);
    }

}
