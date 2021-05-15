package schema_loader;

import com.fasterxml.jackson.databind.node.ArrayNode;
import messages.Json;

public class SchemaLoader {
    /** Class to manager deserialization of schema files */

    private static final String SCHEMA_COLUMN_NAME_KEY = "name";
    private static final String SCHEMA_COLUMN_DATA_TYPE_KEY = "type";

    private final ArrayNode schemaObject;

    private SchemaLoader(String filePath) {
//        this.schemaObject = (ArrayNode) Json.deserializeFromFile(filePath);
        this.schemaObject = (ArrayNode) Json.deserializeFromString(filePath);
    }

    public static SchemaLoader of(String filePath) {
        return new SchemaLoader(filePath);
    }

    public int getNumColumns() {
        return this.schemaObject.size();
    }

    public String getColumnNameByIndex(int i) {
        return this.schemaObject.get(i).get(SCHEMA_COLUMN_NAME_KEY).asText();
    }

    public String getSchemaColumnDataTypeKey(int i) {
        return this.schemaObject.get(i).get(SCHEMA_COLUMN_DATA_TYPE_KEY).asText();
    }
}
