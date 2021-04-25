package transforms;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import templates.Options;

import java.nio.charset.StandardCharsets;

public class UniversalSync extends PTransform<PCollection<PubsubMessage>, PDone> {

    private String outputFile;
    private String outputBqTable;

    public UniversalSync(Options options) {
        this.outputFile = options.getOutputFile();
        this.outputBqTable = options.getBqTable();
    }

    @Override
    public PDone expand(PCollection<PubsubMessage> input) {

        if (!this.outputFile.isEmpty()) {

            input
                    .apply("Converting PubSub messages to String", MapElements
                            .into(TypeDescriptors.strings())
                            .via(message -> new String(message.getPayload(), StandardCharsets.UTF_8)))

                    .apply("Windowing", Window
                            .into(FixedWindows.of(Duration.standardSeconds(200))))

                    .apply("Writing to file", TextIO
                            .write()
                            .to(this.outputFile)
                            .withWindowedWrites()
                            .withNumShards(1));

        }

        if (!this.outputBqTable.isEmpty()) {

            input
                    .apply("Writing to BQ", null);

        }
        return PDone.in(input.getPipeline());
    }

}
