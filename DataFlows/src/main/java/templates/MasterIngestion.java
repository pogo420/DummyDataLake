package templates;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import transforms.UniversalSync;

import java.util.List;

public class MasterIngestion {

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)  // reads arguments --option=value
                .withValidation() // validation of values are done
                .as(Options.class);

        run(options);
    }

    public static class SumString implements SerializableFunction<Iterable<String>, String> {
        @Override
        public String apply(Iterable<String> input) {
            StringBuilder sum = new StringBuilder();
            for (String item : input) {
                sum.append(item).append("\n");
            }
//            System.out.println(sum.toString());
            return sum.toString();
        }
    }

    private static PipelineResult run(Options options) {
        Pipeline pipeline = Pipeline.create(options);

        // Reading from pub sub IO
        PCollection<PubsubMessage> pubSubData = pipeline
                .apply("Reading PubSub Messages", PubsubIO
                        .readMessagesWithAttributes()
                        .fromSubscription(options.getInputSubscription()));

        // Reading schema file
        PCollectionView<List<String>> schema = pipeline
                .apply("Read schema file", TextIO.read().from(options.getSchemaFile()))
                .apply(View.asList())
//                .apply(Combine.globally(new SumString()).asSingletonView())
                ;

        // Master Composite Transform
        pubSubData
                .apply("Master Sync",new UniversalSync(options, schema));

        return pipeline.run();
    }

}
