package cs451.Primitives.Messages;

import java.util.HashSet;

public class Proposal extends Message {
    String delim = ",";
    protected Integer corresponding_round;

    protected HashSet<Integer> proposed_value;

    public Integer getCorresponding_round() {
        return corresponding_round;
    }

    public Proposal(Integer corresponding_round,Integer proposal_number, HashSet<Integer> proposed_value) {
        this.corresponding_round = corresponding_round;
        this.proposal_number = proposal_number;
        this.proposed_value = proposed_value;
    }

    public Proposal(Integer corresponding_round, Integer proposal_number, String proposed_value) {
        this.corresponding_round = corresponding_round;
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
        return "proposal number: " + proposal_number + " -- " + "proposed_value: " + this.setToString(this.proposed_value, this.delim);
    }

    @Override
    public String toPacketString() {
        return "@" + " " + corresponding_round + " " + proposal_number + " " + this.setToString(this.proposed_value, this.delim);
    }

    @Override
    public String getAckMsg() {
        return "+" + " " + proposal_number;
    }
}
