package cs451;

import java.util.Objects;

public class Message {

    public Message(Integer senderId, Integer originalSenderId, Integer seqNumber) {
        this.senderId = senderId;
        this.seqNumber = seqNumber;
        this.originalSenderId = originalSenderId;
    }

    public int getSeqNumber() {
        return seqNumber;
    }

    public int getOriginalSenderId() {
        return originalSenderId;
    }

    public int getSenderId() {
        return senderId;
    }

    protected int senderId;
    protected int originalSenderId;
    protected int seqNumber;

    @Override
    public boolean equals(Object that_) {
        if(that_ instanceof Message) {
            Message that = (Message) that_;
            if(this.senderId == that.senderId) {
                if(this.originalSenderId == that.originalSenderId) {
                    if(this.seqNumber == that.seqNumber) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(senderId, originalSenderId, seqNumber);
    }

    public String toString() {
        return "sender id: " + senderId + " " + "origsender id: " + originalSenderId + "sn: " + seqNumber;
    }
}