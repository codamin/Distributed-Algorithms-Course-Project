package cs451.Primitives.Messages;

import java.nio.ByteBuffer;

public class Ack extends Message{
    public Ack(Integer round, int proposal_number) {
        this.round = round;
        this.proposal_number = proposal_number;
    }

    @Override
    public String toString() {
        return "ack of proposal number: " + proposal_number;
    }
//    @Override
//    public String toPacketString() {
//        return "A" + " " + round + " " + proposal_number;
//    }

    @Override
    public byte[] contentByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(2 + 4 + 4);
        buffer = buffer.putChar('A').putInt(round).putInt(proposal_number);
        return buffer.array();
    }

    @Override
    public byte[] ackByteArray() {
        return ByteBuffer.allocate(6).putChar('a').putInt(proposal_number).array();
    }

//    @Override
//    public String getAckMsg() {
//        return "a" + " " + proposal_number;
//    }
}
