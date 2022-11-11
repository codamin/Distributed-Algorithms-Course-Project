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
        if(that_ instanceof Message) {
            FIFOMessage that = (FIFOMessage) that_;
            if(this.originalSenderId == that.originalSenderId) {
                if(this.msgContent == that.getMsgContent()) {
                    if (this.seqNumber == that.getSeqNumber()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seqNumber, originalSenderId, msgContent);
    }
}
