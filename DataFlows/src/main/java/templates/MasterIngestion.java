package templates;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import transforms.UniversalSync;

public class MasterIngestion {

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)  // reads arguments --option=value
                .withValidation() // validation of values are done
                .as(Options.class);

        run(options);
    }

    private static PipelineResult run(Options options){
        Pipeline pipeline = Pipeline.create(options);

        // Reading from pub sub IO
        PCollection<PubsubMessage> pubSubData = pipeline
                .apply(PubsubIO
                        .readMessagesWithAttributes()
                        .fromSubscription(options.getInputSubscription()));


        pubSubData
                .apply("Master Sync",new UniversalSync(options));

        return pipeline.run();
    }

}
