package cs451.Primitives.Messages;

public class Ack extends Message{
    public Ack(int proposal_number) {
        this.proposal_number = proposal_number;
    }

    @Override
    public String toString() {
        return "ack of proposal number: " + proposal_number;
    }
    @Override
    public String toPacketString() {
        return "A" + " " + proposal_number;
    }

    @Override
    public String getAckMsg() {
        return "a" + " " + proposal_number;
    }
}
