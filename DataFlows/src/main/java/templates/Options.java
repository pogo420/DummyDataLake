package templates;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;


public interface Options extends PipelineOptions {

    @Description("output file path")
    String getOutputFile();
    void setOutputFile(String value);

    @Description("output topic name")
    String getOutputTopic();
    void setOutputTopic(String value);

    @Description("output bq table name")
    String getBqTable();
    void setBqTable(String value);

    @Description("input subscription")
    String getInputSubscription();
    void setInputSubscription(String value);

}
