package cs451.Primitives.Messages;

import java.nio.ByteBuffer;
import java.util.HashSet;

public abstract class Message {
    protected Integer proposal_number;
    public Integer getRound() {return round;}
    protected Integer round;
    public Integer getProposal_number() {return proposal_number;}
    abstract public byte[] ackByteArray();
    abstract public byte[] contentByteArray();
}