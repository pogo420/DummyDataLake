package templates;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;


public interface Options extends PipelineOptions {

    @Description("output file path")
    String getOutputFile();
    void setOutputFile(String value);

    @Description("input subscription")
    String getInputSubscription();
    void setInputSubscription(String value);

}
