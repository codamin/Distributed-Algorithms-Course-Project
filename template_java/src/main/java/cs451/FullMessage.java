package cs451;

import java.util.Objects;

public class FullMessage {
    public Integer senderId;
    public Message fifoMessage;

    public FullMessage(Integer senderId, Message fifoMessage) {
        this.senderId = senderId;
        this.fifoMessage = fifoMessage;
    }

    @Override
    public boolean equals(Object that_) {
        if(that_ instanceof FullMessage) {
            FullMessage that = (FullMessage) that_;
            if(this.fifoMessage.equals(that.fifoMessage)) {
                if(this.senderId.equals(that.senderId)) {
                    return true;
                }
            }
        }
        return false;
    }

    public String toString() {
        return senderId + "#" + fifoMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(senderId, fifoMessage);
    }
}