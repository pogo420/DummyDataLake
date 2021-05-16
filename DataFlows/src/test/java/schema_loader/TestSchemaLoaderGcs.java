package schema_loader;

import org.junit.jupiter.api.Test;

public class TestSchemaLoaderGcs {

    @Test
    public void testFileRead(){
        String schema = SchemaLoaderGcs.loadSchema("/home/lenovo/Projects/DummyDataLake/DataFlows/infrastructure/bq-schemas/sensor-data.json");
        System.out.println(schema);
        assert !schema.isEmpty();
    }
}
