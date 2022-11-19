package cs451;

import java.util.Objects;

public class FIFOMessage extends Message {

    public String getMsgContent() {
        return msgContent;
    }

    private String msgContent;

    public FIFOMessage(Integer seqNumber, Integer originalSenderId, String msgContent) {
        super(seqNumber, originalSenderId);
        this.msgContent = msgContent;
    }

    @Override
    public boolean equals(Object that_) {
        if(that_ instanceof FIFOMessage) {
            FIFOMessage that = (FIFOMessage) that_;
            if(this.originalSenderId == that.originalSenderId) {
                if (this.seqNumber == that.seqNumber) {
                    return true;
                }
            }
        }
        return false;
    }

    public String toString() {
        return originalSenderId + "#" + seqNumber + "#" + msgContent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seqNumber, originalSenderId);
    }
}
