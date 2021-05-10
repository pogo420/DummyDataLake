package transforms;

import com.fasterxml.jackson.databind.node.ObjectNode;
import messages.IngestionMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;

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

        input.apply("Adding Processing Time", ParDo.of(addingProcessingTime()));
        return null;
    }


    private DoFn<IngestionMessage, IngestionMessage> addingProcessingTime() {
        return new DoFn<IngestionMessage, IngestionMessage>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                ObjectNode objectNode = c.element().getPayload();
                ObjectNode payload = (ObjectNode) objectNode.get(PAYLOAD);
                ObjectNode payloadWithTimeStamp = payload.put(PROCESSING_TIME, c.timestamp().toString());
                IngestionMessage ingestionMessageWithTimeStamp = IngestionMessage.setPayload(payloadWithTimeStamp);
                c.output(ingestionMessageWithTimeStamp);
            }
        };
    }
}
