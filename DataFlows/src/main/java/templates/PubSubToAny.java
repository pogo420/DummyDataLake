package templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class PubSubToAny {

    public interface Options extends PipelineOptions{
        // interface for defining options

        @Description("input subscription")
        ValueProvider<String> getInputSubscription();
        void setInputSubscription(ValueProvider<String> value);

        @Description("output file path")
        ValueProvider<String> getOutputFile();
        void setOutputFile(ValueProvider<String> value);

        @Description("output topic")
        ValueProvider<String> getOutputTopic();
        void setOutputTopic(ValueProvider<String> value);

        @Description("output BQ table")
        ValueProvider<String> getOutputBqTable();
        void setOutputBqTable(ValueProvider<String> value);

        @Description("output BT table")
        ValueProvider<String> getOutputBtTable();
        void setOutputBtTable(ValueProvider<String> value);

    }

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

            // Writing into text file
            pubSubData
                    .apply("Converting PubSub messages to String", MapElements
                    .into(TypeDescriptors.strings())
                    .via(PubsubMessage::toString))

                    .apply("Windowing", Window
                            .into(FixedWindows.of(Duration.standardMinutes(2))))

                    .apply("Writing to file", TextIO
                            .write()
                            .to(options.getOutputFile())
                            .withWindowedWrites()
                            .withNumShards(1)
                    );
        pipeline.run();
    }
}
