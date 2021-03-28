package templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import transforms.UniversalSink;

public class PubSubToAny {

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)  // reads arguments --option=value
                .withValidation() // validation of values are done
                .as(Options.class);

        run(options);
    }

    private static void run(Options options){
        Pipeline pipeline = Pipeline.create(options);

        // Reading from pub sub IO
        PCollection<PubsubMessage> pubSubData = pipeline
                .apply(PubsubIO
                        .readMessagesWithAttributes()
                        .fromSubscription(options.getInputSubscription()));

        pubSubData.apply("Sink processing", UniversalSink.build(options));
            // Writing into text file
//            pubSubData
//                    .apply("Converting PubSub messages to String", MapElements
//                    .into(TypeDescriptors.strings())
//                    .via(PubsubMessage::toString))
//
//                    .apply("Windowing", Window
//                            .into(FixedWindows.of(Duration.standardMinutes(2))))
//
//                    .apply("Writing to file", TextIO
//                            .write()
//                            .to(options.getOutputFile())
//                            .withWindowedWrites()
//                            .withNumShards(1)
//                    );
        pipeline.run();
    }
}
