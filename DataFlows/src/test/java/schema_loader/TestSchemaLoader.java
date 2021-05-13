package schema_loader;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

public class TestSchemaLoader {
    @Test
    public void testSchemaData(){
        int numColumns = SchemaLoader.of("/home/lenovo/Projects/DummyDataLake/" +
                "DataFlows/infrastructure/bq-schemas/sensor-data.json").getNumColumns();

        assert numColumns== 3;
    }

    @Test
    public void testColumnName(){
        String columnName = SchemaLoader.of("/home/lenovo/Projects/DummyDataLake/" +
                "DataFlows/infrastructure/bq-schemas/sensor-data.json").getColumnNameByIndex(2);

        assert columnName.equals("sensorId");
    }

}
