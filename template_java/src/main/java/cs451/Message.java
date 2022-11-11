package cs451;

import java.util.Objects;

public class Message {

    public Message(Integer seqNumber, Integer originalSenderId) {

        this.seqNumber = seqNumber;
        this.originalSenderId = originalSenderId;
    }

    public int getSeqNumber() {
        return seqNumber;
    }

    public int getOriginalSenderId() {
        return originalSenderId;
    }


    protected int originalSenderId;
    protected int seqNumber;
}