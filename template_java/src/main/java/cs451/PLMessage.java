package cs451;

import java.util.Objects;

public class PLMessage extends Message{
    public Integer getSenderId() {
        return senderId;
    }

    private Integer senderId;

    public PLMessage(Integer senderId, Integer originalSenderId, Integer seqNumber) {
        super(seqNumber, originalSenderId);
        this.senderId = senderId;
    }

    @Override
    public boolean equals(Object that_) {
        if(that_ instanceof PLMessage) {
            PLMessage that = (PLMessage) that_;
            if(this.originalSenderId == that.originalSenderId) {
                if(this.senderId == that.senderId) {
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
        return Objects.hash(senderId, seqNumber, originalSenderId);
    }
}
