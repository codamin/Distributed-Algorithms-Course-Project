package cs451.Primitives.Messages;

import java.util.HashSet;

public class Proposal extends Message {
    String delim = ",";

    protected HashSet<Integer> proposed_value;

    public Proposal(Integer proposal_number, HashSet<Integer> proposed_value) {
        this.proposal_number = proposal_number;
        this.proposed_value = proposed_value;
    }

    public Proposal(Integer proposal_number, String proposed_value) {
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
        return "@" + " " + proposal_number + " " +  this.setToString(this.proposed_value, this.delim);
    }

    @Override
    public String getAckMsg() {
        return "+" + " " + proposal_number;
    }
}
