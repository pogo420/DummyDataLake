package coders;

import messages.IngestionMessage;
import org.apache.beam.sdk.coders.Coder;

public class Coders {
    /** Class for defining custom coders */

    public static Coder<IngestionMessage> ingestionMessage() {
        return CustomCode.of(IngestionMessage.class)
                .encoder(IngestionMessage::messageSerial)
                .decoder(IngestionMessage::messageDeSerial)
                .build();
    }
}
