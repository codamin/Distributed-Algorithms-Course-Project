package cs451.Primitives.Messages;

import java.nio.ByteBuffer;

public class Decided extends Message{
    public Decided(Integer round) {
        this.round = round;
    }

    @Override
    public String toString() {
        return "decision of round: " + round;
    }

    @Override
    public byte[] contentByteArray() {
        ByteBuffer buffer = ByteBuffer.allocate(2 + 4);
        buffer = buffer.putChar('D').putInt(round);
        return buffer.array();
    }

    @Override
    public byte[] ackByteArray() {
        return ByteBuffer.allocate(6).putChar('d').putInt(round).array();
    }
}
