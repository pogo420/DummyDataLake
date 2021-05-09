package transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import messages.IngestionMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import static messages.IngestionMessage.PROCESSING_TIME;
import static messages.IngestionMessage.PAYLOAD;


public class Validations extends PTransform<PCollection<IngestionMessage>, PCollectionTuple> {

    private String schemaPath;

    private Validations(String schemaPath) {
        this.schemaPath = schemaPath;

    }

    public static Validations with(String schemaPath) {
        return new Validations(schemaPath);
    }

    @Override
    public PCollectionTuple expand(PCollection<IngestionMessage> input) {

        input.apply("Adding Processing Time", ParDo.of(
                new DoFn<IngestionMessage, IngestionMessage>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        ObjectNode objectNode = c.element().getPayload();
                        JsonNode payload = objectNode.get(PAYLOAD);



                        c.output(null);
                    }
                }
            )
        );
        return null;
    }


}
