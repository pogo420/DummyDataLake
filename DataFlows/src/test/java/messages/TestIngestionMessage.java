package messages;

import io.vavr.Tuple2;
import org.junit.jupiter.api.Test;
import schema_loader.SchemaLoader;

public class TestIngestionMessage {

    @Test
    public void testValidatorFailure(){

        String message = "{\n" +
                "\t\"_pl\": {\n" +
                "            \"userI\": \"abc-ooo-9097\",\n" +
                "            \"sensorValue\": 0.908,\n" +
                "            \"sensorId\": \"pp-p-00opouu\"\n" +
                "        }\n" +
                "    }";

        IngestionMessage ingestionMessage = Json.deserialize(message, IngestionMessage.class);
        Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>> validatorResponse = IngestionMessage.validator(MessageWrapper.wrap(ingestionMessage), SchemaLoader.of("/home/lenovo/Projects/DummyDataLake/" +
                "DataFlows/infrastructure/bq-schemas/sensor-data.json"));

        assert validatorResponse._2.getFailureFlag();
    }

    @Test
    public void testValidatorSuccess(){

        String message = "{\n" +
                "\t\"_pl\": {\n" +
                "            \"userId\": \"abc-ooo-9097\",\n" +
                "            \"sensorValue\": 0.908,\n" +
                "            \"sensorId\": \"pp-p-00opouu\"\n" +
                "        }\n" +
                "    }";

        IngestionMessage ingestionMessage = Json.deserialize(message, IngestionMessage.class);
        Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>> validatorResponse = IngestionMessage.validator(MessageWrapper.wrap(ingestionMessage), SchemaLoader.of("/home/lenovo/Projects/DummyDataLake/" +
                "DataFlows/infrastructure/bq-schemas/sensor-data.json"));

        assert !validatorResponse._2.getFailureFlag();
    }

}
