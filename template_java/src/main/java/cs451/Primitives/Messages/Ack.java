package cs451.Primitives.Messages;

public class Ack extends Message{
    public Ack(Integer round, int proposal_number) {
        this.round = round;
        this.proposal_number = proposal_number;
    }

    @Override
    public String toString() {
        return "ack of proposal number: " + proposal_number;
    }
    @Override
    public String toPacketString() {
        return "A" + " " + round + " " + proposal_number;
    }

    @Override
    public String getAckMsg() {
        return "a" + " " + proposal_number;
    }
}
