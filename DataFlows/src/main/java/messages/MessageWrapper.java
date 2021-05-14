package messages;

import java.io.Serializable;

public class MessageWrapper<T> implements Serializable {

    private final T messageObj;
    private boolean isFailure;
    private String failureMessage;

    private MessageWrapper(T messageObj) {
        this.messageObj = messageObj;
    }

    public static <T> MessageWrapper<T> wrap(T messageObj){
        return new MessageWrapper<T>(messageObj);
    }

    public void setFailureFlag(boolean failureFlag) {
        this.isFailure = failureFlag;
    }

    public boolean getFailureFlag() {
        return this.isFailure;
    }

    public String getFailureMessage() {
        return failureMessage;
    }

    public void setFailureMessage(String failureMessage) {
        this.failureMessage = failureMessage;
    }

    public T getMessageObj() {
        return this.messageObj;
    }
}
