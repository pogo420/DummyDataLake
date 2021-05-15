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
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import templates.Options;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_APPEND;
import static transforms.Validations.FAILURE;
import static transforms.Validations.SUCCESS;

public class UniversalSync extends PTransform<PCollection<PubsubMessage>, PDone> {
    /** Class for Composite Transformation: Universal sink*/

    private String outputFile;
    private String outputBqTable;
    private PCollectionView<List<String>> schema;

    public UniversalSync(Options options, PCollectionView<List<String>> schema ) {
        this.outputFile = options.getOutputFile();
        this.outputBqTable = options.getOutputBqTable();
        this.schema = schema;
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

        PCollectionTuple validatedMessage = ingestionMessageCoded
                .apply("Validate Schema", Validations.with(this.schema));

        // TODO APi dev for dataflow calls and plan other TODOs
        if (!this.outputFile.isEmpty()) {

            // Handing success
            validatedMessage.get(SUCCESS)
                    .apply("Converting PubSub messages to String", MapElements
                            .into(TypeDescriptors.strings())
                            .via(IngestionMessage::messageSerial))

                    .apply("Windowing", Window
                            .into(FixedWindows.of(Duration.standardSeconds(200))))

                    .apply("Writing to file", TextIO
                            .write()
                            .to(this.outputFile + "success_")
                            .withWindowedWrites()
                            .withNumShards(1));

            // Handing failure
            validatedMessage.get(FAILURE)
                    .apply("Error Data Processing", MapElements
                            .into(TypeDescriptors.strings())
                            .via(message-> message._1.messageSerial()))
                    .apply("Windowing", Window
                            .into(FixedWindows.of(Duration.standardSeconds(200))))

                    .apply("Writing to file", TextIO
                            .write()
                            .to(this.outputFile + "failure_")
                            .withWindowedWrites()
                            .withNumShards(1));
        }
// Not supported in free trial
        /*
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
         */
        return PDone.in(input.getPipeline());
    }

}
