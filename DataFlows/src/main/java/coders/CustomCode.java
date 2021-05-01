package coders;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.compress.utils.IOUtils;
import utils.functions.SerialFunction;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class CustomCode<T> extends CustomCoder<T> {

    public static class Builder<T> {

        private TypeDescriptor<T> type;
        private SerialFunction<T, String> encoder;
        private SerialFunction<String, T> decoder;

        private Builder<T> of(TypeDescriptor<T> type){
            this.type = type;
            return this;
        }

        public Builder<T> encoder(SerialFunction<T, String> encoder){
            this.encoder = encoder;
            return this;
        }

        public Builder<T> decoder(SerialFunction<String, T> decoder){
            this.decoder = decoder;
            return this;
        }

        public CustomCode<T> build(){
            return new CustomCode<T>(type, encoder, decoder);
        }

    }

    private TypeDescriptor<T> type;
    private SerialFunction<T, String> encoder;
    private SerialFunction<String, T> decoder;

    private CustomCode(TypeDescriptor<T> type,
                       SerialFunction<T, String> encoder,
                       SerialFunction<String, T> decoder) {
        this.type = type;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    public static <T> Builder<T> of(Class<T> class_) {
        return new Builder<T>().of(TypeDescriptor.of(class_));
    }

    @Override
    public void encode(T value, OutputStream outStream) throws CoderException, IOException {
        outStream.write(encoder.apply(value).getBytes()); // serialization will be done by the function.
    }

    @Override
    public T decode(InputStream inStream) throws CoderException, IOException {
        String serializedMessage = new String(IOUtils.toByteArray(inStream), StandardCharsets.UTF_8);
        return decoder.apply(serializedMessage); // deserialization will be done by the function.
    }

}
