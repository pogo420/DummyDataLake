package schema_loader;

import com.google.common.io.CharStreams;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

public class SchemaLoaderGcs implements Serializable {
    /** Class for managing Beam's FileSystems API*/
    public static String loadSchema(String filePath) {
        MatchResult.Metadata metadata;
        try {
            // Searching for file
            metadata = FileSystems.matchSingleFileSpec(filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String schema;

        try {
            // Reading contents for file
            schema = CharStreams.toString(
                Channels.newReader(
                        FileSystems.open(metadata.resourceId()),
                        StandardCharsets.UTF_8.name()
                )
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return schema;
    }
}
