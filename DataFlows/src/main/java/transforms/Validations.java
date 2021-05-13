package transforms;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import messages.IngestionMessage;
import messages.MessageWrapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import schema_loader.SchemaLoader;

import static messages.IngestionMessage.PROCESSING_TIME;
import static messages.IngestionMessage.PAYLOAD;


public class Validations extends PTransform<PCollection<IngestionMessage>, PCollectionTuple> {

    // TODO sample schema in json file - DONE
    // TODO create class: Schema Json to Object DONE
    // TODO validation mapElements: each row from Schema and check if it present in IngestionMessage
    // TODO Wrapper class on IngestionMessage to get message and error message. DONE
    // TODO create tag tuple: SUCCESS and FAIL
    // TODO Create tuple output(tuple tag, message) and (tuple tag, message, error message)

    private String schemaPath;

    private Validations(String schemaPath) {
        this.schemaPath = schemaPath;

    }

    public static Validations with(String schemaPath) {
        return new Validations(schemaPath);
    }

    @Override
    public PCollectionTuple expand(PCollection<IngestionMessage> input) {

        input
                .apply("Adding Processing Time", ParDo.of(addingProcessingTime()))
                .apply("Wrapping Ingestion Message", wrapMessage())
                .apply("Validating Schema", MapElements
                        .into(new TypeDescriptor<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {})
                        .via(tuple -> {
                            IngestionMessage.validator(tuple._1, SchemaLoader.of(this.schemaPath));
                                    return null;
                        })
                )

        ;
        return null;
    }

    private MapElements<IngestionMessage, Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> wrapMessage() {
        return MapElements
                .into(new TypeDescriptor<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {})
                .via(message -> Tuple.of(message, MessageWrapper.wrap(message)));
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
