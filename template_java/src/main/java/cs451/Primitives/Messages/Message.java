package cs451.Primitives.Messages;

import java.util.HashSet;
import java.util.Objects;

public abstract class Message {
    //    @Override
//    public boolean equals(Object that_) {
//        if(that_ instanceof Message) {
//            Message that = (Message) that_;
//            if(this.senderId == that.senderId) {
//                if(this.proposal_number == that.proposal_number) {
//                    return true;
//                }
//            }
//        }
//        return false;
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(senderId, proposal_number);
//    }
    protected Integer proposal_number;

    public Integer getProposal_number() {return proposal_number;}
    abstract public String toPacketString();
    abstract public String getAckMsg();
    public String setToString(HashSet<Integer> proposed_value, String delim) {
        String out = "";
        for(Integer elem: proposed_value) {
            out += elem.toString();
            out += delim;
        }
        // remove last delim
        out = out.substring(0, out.length()-1);
        return out;
    }
}