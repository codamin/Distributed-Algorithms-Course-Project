package cs451.Primitives.Messages;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class Nack extends Message{
    String delim = ",";

    protected HashSet<Integer> accepted_value;

    public Nack(Integer round, Integer proposal_number, HashSet<Integer> accepted_value) {
        this.round = round;
        this.proposal_number = proposal_number;
        this.accepted_value = accepted_value;
    }

    public Nack(Integer round, Integer proposal_number, String accepted_value) {
        this.round = round;
        this.proposal_number = proposal_number;
        this.accepted_value = new HashSet<>();
        for(String s: accepted_value.split(this.delim)) {
            this.accepted_value.add(Integer.parseInt(s));
        }
    }

    public HashSet<Integer> getAccepted_value() {
        return accepted_value;
    }

    @Override
    public String toString() {
        return "ack of proposal number: " + proposal_number + " -- " + "accepted_value: " + this.accepted_value;
    }
//    @Override
//    public String toPacketString() {
//        return "N" + " " + round + " " + proposal_number + " " +  this.setToString(this.accepted_value, this.delim);
//    }

    @Override
    public byte[] contentByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(2 + 4 + 4 + 4 + 4*accepted_value.size());
        buffer = buffer.putChar('N').putInt(round).putInt(proposal_number).putInt(accepted_value.size());
        for(Integer elem: accepted_value) {
            buffer = buffer.putInt(elem);
        }
        return buffer.array();
    }

//    @Override
//    public String getAckMsg() {
//        return "n" + " " + proposal_number;
//    }

    @Override
    public byte[] ackByteArray() {
        return ByteBuffer.allocate(6).putChar('n').putInt(proposal_number).array();
    }
}
