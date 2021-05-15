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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.Optional;

import static messages.IngestionMessage.PROCESSING_TIME;
import static messages.IngestionMessage.PAYLOAD;


public class Validations extends PTransform<PCollection<IngestionMessage>, PCollectionTuple> {
    /** Transformation to validate a message */

    private static final Logger LOG = LoggerFactory.getLogger(Validations.class);

    private PCollectionView<List<String>> schemaPath;

    public static final TupleTag<IngestionMessage> SUCCESS = new TupleTag<IngestionMessage>() {};
    public static final TupleTag<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> FAILURE = new TupleTag<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {};

    private Validations(PCollectionView<List<String>> schemaPath) {
        this.schemaPath = schemaPath;
    }

    public static Validations with(PCollectionView<List<String>> schemaPath) {
        return new Validations(schemaPath);
    }

    @Override
    public PCollectionTuple expand(PCollection<IngestionMessage> input) {

        return input
//                .apply("Adding Processing Time", ParDo.of(addingProcessingTime()))
                .apply("Wrapping Ingestion Message", wrapMessage())
                .apply("Validating Schema with SI", validateSchema())
//                .apply("Validating Schema", validateSchema())
                .apply("Tagging messages", tagMessages(SUCCESS, FAILURE));

    }

    private ParDo.SingleOutput<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>, Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> validateSchema() {
        return ParDo.of(new DoFn<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>, Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>> element = c.element();
                List<String> schema = c.sideInput(schemaPath);
                for (String i: schema){
                LOG.info("values: "+ i);
                }
                Optional<String> reduce = schema.stream().reduce((x, y) -> x + y);
                Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>> validatedMessage = IngestionMessage.validator(element._2, SchemaLoader.of(reduce.get()));
                c.output(validatedMessage);
            }
        }).withSideInputs(schemaPath);
    }


    private ParDo.MultiOutput<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>, IngestionMessage> tagMessages(
            TupleTag<IngestionMessage> success,
            TupleTag<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> failure) {
        // Tagging message based on validation status
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


//    private MapElements<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>, Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> validateSchema() {
//        // Validating schema
//        return MapElements
//                .into(new TypeDescriptor<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {
//                })
//                .via(tuple -> IngestionMessage.validator(tuple._2, SchemaLoader.of(this.schemaPath)));
//    }

    private MapElements<IngestionMessage, Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>> wrapMessage() {
        // Wrapping ingestion message to store, process status
        return MapElements
                .into(new TypeDescriptor<Tuple2<IngestionMessage, MessageWrapper<IngestionMessage>>>() {})
                .via(message -> Tuple.of(message, MessageWrapper.wrap(message)));
    }

    private DoFn<IngestionMessage, IngestionMessage> addingProcessingTime() {
        // adding processing time as a key
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
