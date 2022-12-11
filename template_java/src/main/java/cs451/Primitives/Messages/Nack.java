package cs451.Primitives.Messages;

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
        return "ack of proposal number: " + proposal_number + " -- " + "accepted_value: " + this.setToString(this.accepted_value, this.delim);
    }
    @Override
    public String toPacketString() {
        return "N" + " " + round + " " + proposal_number + " " +  this.setToString(this.accepted_value, this.delim);
    }

    @Override
    public String getAckMsg() {
        return "n" + " " + proposal_number;
    }
}
