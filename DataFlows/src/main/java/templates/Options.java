package templates;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions {
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
