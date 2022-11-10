package cs451;

public class Message {
    public Message(int seqNumber, Integer originalSenderId, String msgContent) {
        this.seqNumber = seqNumber;
        this.msgContent = msgContent;
        this.originalSenderId = originalSenderId;
    }
    public int getSeqNumber() {
        return seqNumber;
    }

    public String getMsgContent() {
        return msgContent;
    }

    public int getOriginalSenderId() {
        return originalSenderId;
    }

    @Override
    public boolean equals(Object that_) {
        if(that_ instanceof Message) {
            Message that = (Message) that_;
            if(this.originalSenderId == that.originalSenderId) {
                if(this.getSeqNumber() == that.getSeqNumber()) {
                    return true;
                }
            }
         }
        return false;
    }

    private int originalSenderId;
    private int seqNumber;
    private String msgContent;
}