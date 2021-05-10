package transforms;

import coders.Coders;
import com.google.api.services.bigquery.model.TableRow;
import messages.IngestionMessage;
import messages.Json;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import templates.Options;

import java.nio.charset.StandardCharsets;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;

public class UniversalSync extends PTransform<PCollection<PubsubMessage>, PDone> {

    private String outputFile;
    private String outputBqTable;

    public UniversalSync(Options options) {
        this.outputFile = options.getOutputFile();
        this.outputBqTable = options.getOutputBqTable();
    }

    @Override
    public PDone expand(PCollection<PubsubMessage> input) {

        PCollection<IngestionMessage> ingestionMessage = input
                .apply("Converting to Ingestion Message", MapElements
                        .into(TypeDescriptor.of(IngestionMessage.class))
                        .via(message -> IngestionMessage.messageDeSerial(
                                new String(message.getPayload(), StandardCharsets.UTF_8)
                        )));

        PCollection<IngestionMessage> ingestionMessageCoded = ingestionMessage
                .setCoder(Coders.ingestionMessage());

        // TODO add the validation trasform
        // TODO Success > move ahead and failure > failure_file_name
        // TODO Test
        // TODO APi dev for dataflow calls and plan other TODOs
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
// Not supported in free trial
        if (!this.outputBqTable.isEmpty()) {

            ingestionMessageCoded.apply("Write to BQ", BigQueryIO
                    .<IngestionMessage>write()
                    .withFormatFunction(
                            message -> Json.getMapper().convertValue(message.getPayload(), TableRow.class)
                    )
                    .to(this.outputBqTable)
                    .withCreateDisposition(CREATE_NEVER)
                    .withWriteDisposition(WRITE_APPEND)
            );

        }
        return PDone.in(input.getPipeline());
    }

}
