package cs451.Primitives.Messages;

import java.nio.ByteBuffer;
import java.util.HashSet;

public abstract class Message {
    protected Integer proposal_number;

    public Integer getRound() {return round;}
    protected Integer round;

    public Integer getProposal_number() {return proposal_number;}
//    abstract public String toPacketString();
//    abstract public String getAckMsg();

    abstract public byte[] ackByteArray();
    abstract public byte[] contentByteArray();
//    public String setToString(HashSet<Integer> proposed_value, String delim) {
//        String out = "";
//        for(Integer elem: proposed_value) {
//            out += elem.toString();
//            out += delim;
//        }
//        // remove last delim
//        out = out.substring(0, out.length()-1);
//        return out;
//    }
}