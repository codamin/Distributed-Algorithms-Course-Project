package cs451.Primitives.Messages;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class Proposal extends Message {
    String delim = ",";
    protected HashSet<Integer> proposed_value;

    public Proposal(Integer round,Integer proposal_number, HashSet<Integer> proposed_value) {
        this.round = round;
        this.proposal_number = proposal_number;
        this.proposed_value = proposed_value;
    }

    public Proposal(Integer round, Integer proposal_number, String proposed_value) {
        this.round = round;
        this.proposal_number = proposal_number;
        this.proposed_value = new HashSet<>();
        for(String s: proposed_value.split(this.delim)) {
            this.proposed_value.add(Integer.parseInt(s));
        }
    }

    public HashSet<Integer> getProposed_value() {
        return proposed_value;
    }

    @Override
    public String toString() {
        return "round: " + round + " number: " + proposal_number + " value: " + this.proposed_value;
    }

//    @Override
//    public String toPacketString() {
//        return "@" + " " + round + " " + proposal_number + " " + this.setToString(this.proposed_value, this.delim);
//    }

    @Override
    public byte[] contentByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(2 + 4 + 4 + 4 + 4*proposed_value.size());
        buffer = buffer.putChar('@').putInt(round).putInt(proposal_number).putInt(proposed_value.size());
        for(Integer elem: proposed_value) {
            buffer = buffer.putInt(elem);
        }
        return buffer.array();
    }

//    @Override
//    public String getAckMsg() {
//        return "+" + " " + proposal_number;
//    }

    @Override
    public byte[] ackByteArray() {
        return ByteBuffer.allocate(6).putChar('+').putInt(proposal_number).array();
    }
}
