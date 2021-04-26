package utils.functions;

import java.io.Serializable;
import java.util.function.Function;

public interface SerialFunction<A, B> extends Function<A, B>, Serializable {
}
