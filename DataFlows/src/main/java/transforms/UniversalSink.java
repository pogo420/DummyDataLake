package transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import templates.Options;

public class UniversalSink extends PTransform<PCollection<PubsubMessage>, PCollection<Void>> {

    private String outputFile;
    private String outputTopic;
    private String outputBqTable;
    private String outputBtTable;

    private UniversalSink(){}

    private UniversalSink(String outputFile,
                          String outputTopic,
                          String outputBqTable,
                          String outputBtTable){
        ;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public void setOutputBqTable(String outputBqTable) {
        this.outputBqTable = outputBqTable;
    }

    public void setOutputBtTable(String outputBtTable) {
        this.outputBtTable = outputBtTable;
    }

    public static UniversalSink build(Options options){

            return null;
    }

    @Override
    public PCollection<Void> expand(PCollection<PubsubMessage> input) {
        return null;
    }
}
