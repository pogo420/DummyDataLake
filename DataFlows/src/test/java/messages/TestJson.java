package messages;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;


public class TestJson {

    @Test
    public void testDeserializeFromFile(){
        JsonNode schemaClass = Json.deserializeFromFile("/home/lenovo/Projects/DummyDataLake/" +
                "DataFlows/infrastructure/bq-schemas/sensor-data.json");
        assert schemaClass.isArray();
    }
}
