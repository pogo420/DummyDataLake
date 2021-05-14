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
import org.apache.beam.sdk.values.*;
import schema_loader.SchemaLoader;

import static messages.IngestionMessage.PROCESSING_TIME;
import static messages.IngestionMessage.PAYLOAD;


public class Validations extends PTransform<PCollection<IngestionMessage>, PCollectionTuple> {
    /** Transformation to validate a message */

    private String schemaPath;

    public static final TupleTag<IngestionMessage> SUCCESS = new TupleTag<IngestionMessage>() {};
    public static final TupleTag<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> FAILURE = new TupleTag<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {};

    private Validations(String schemaPath) {
        this.schemaPath = schemaPath;
    }

    public static Validations with(String schemaPath) {
        return new Validations(schemaPath);
    }

    @Override
    public PCollectionTuple expand(PCollection<IngestionMessage> input) {

        return input
                .apply("Adding Processing Time", ParDo.of(addingProcessingTime()))
                .apply("Wrapping Ingestion Message", wrapMessage())
                .apply("Validating Schema", validateSchema())
                .apply("Tagging messages", tagMessages(SUCCESS, FAILURE));

    }

    private ParDo.MultiOutput<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>, IngestionMessage> tagMessages(
            TupleTag<IngestionMessage> success,
            TupleTag<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> failure) {
        return ParDo.of(new DoFn<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>, IngestionMessage>()
        {
            @ProcessElement
            public void processElement(ProcessContext c){
                Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>> element = c.element();
                if(element._2.getFailureFlag()){
                    c.output(FAILURE, element);
                    }
                else {
                    c.output(SUCCESS, element._1);
                    }
                }
            }).withOutputTags(success, TupleTagList.of(failure));
    }


    private MapElements<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>, Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> validateSchema() {
        return MapElements
                .into(new TypeDescriptor<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {
                })
                .via(tuple -> IngestionMessage.validator(tuple._2, SchemaLoader.of(this.schemaPath)));
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
