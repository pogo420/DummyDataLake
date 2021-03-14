package templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;

public class TestTemplate {

    private interface Options extends PipelineOptions {
        // interface for defining options
        @Description("input file path")
        ValueProvider<String> getInputFile();
        void setInputFile(ValueProvider<String> value);

        @Description("output file path")
        ValueProvider<String> getOutputFile();
        void setOutputFile(ValueProvider<String> value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                            .fromArgs(args)  // reads arguments --option=value
                            .withValidation() // validation of values are done
                            .as(Options.class);
        
        run(options);
    }

    private static void run(Options options) {
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("Reading files", TextIO.read().from(options.getInputFile()))
                .apply("Reading files", TextIO.write().to(options.getOutputFile()));

        pipeline.run();
    }
}
