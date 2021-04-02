package transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class UniversalSync extends PTransform<PCollection<PubsubMessage>,PCollection<Void>> {


    @Override
    public PCollection<Void> expand(PCollection<PubsubMessage> input) {
        return null;
    }
}
