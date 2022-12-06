package cs451.Primitives;

import cs451.Host;
import cs451.Primitives.Messages.Ack;
import cs451.Primitives.Messages.Message;
import cs451.Primitives.Messages.Nack;
import cs451.Primitives.Messages.Proposal;

import java.util.HashSet;

public class Consensus {
    boolean active;
    Integer ack_count;
    Integer nack_count;
    Integer active_proposal_number;

    Integer f;
    private HashSet<Integer> proposed_value;

    private Host broadcaster;
    private BEChannel beChannel;
    public Consensus(Host broadcaster,Integer NUMPROC, Integer NUMMSG) {
        this.broadcaster = broadcaster;

        this.active = false;
        this.ack_count = 0;
        this.nack_count = 0;
        this.active_proposal_number = 0;
        this.proposed_value = null;
        this.f = (NUMPROC-1)/2;

        this.beChannel = new BEChannel(this.broadcaster.getHostsList(), this, this.broadcaster, NUMPROC, NUMMSG);
        this.beChannel.startThreads();
    }

    public void propose(HashSet<Integer> proposal) {
        this.proposed_value = proposal;
        this.active = true;

        this.active_proposal_number += 1;
        this.ack_count = 0;
        this.nack_count = 0;
        this.beChannel.be_broadcast(new Proposal(active_proposal_number, (HashSet<Integer>) proposed_value.clone()));
    }

    public void consensus_ack(Integer proposal_number) {
        System.out.println("rcvd ack for proposal: " + proposal_number + " active_proposal_number = " + this.active_proposal_number);
        if(proposal_number == this.active_proposal_number) {
            this.ack_count += 1;

            this.handle_rcvd_msg();
        }
    }

    public void consensus_nack(Integer proposal_number, HashSet value) {
        System.out.println("rcvd nack for proposal: " + proposal_number + " active_proposal_number = " + this.active_proposal_number);
        if(proposal_number == this.active_proposal_number) {
            this.proposed_value.addAll(value);
            this.nack_count += 1;

            this.handle_rcvd_msg();
        }
    }

    public void get_proposal(String sourceIP, Integer sourcePort, Integer proposal_number, HashSet<Integer> incoming_proposed_value) {
        System.out.println("rcvd proposal with number: " + proposal_number);
        if(incoming_proposed_value.containsAll(this.proposed_value)) {
            //send ack
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Ack(proposal_number));
        }
        else {
            //send nack
            this.proposed_value.addAll(incoming_proposed_value);
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Nack(proposal_number, this.proposed_value));
        }
    }

    public void handle_rcvd_msg() {
        System.out.println("handling rcvd ack/nack:");
        System.out.println("active: " + this.active);
        System.out.println("ack_count: " + this.ack_count);
        System.out.println("nack_count: " + this.nack_count);

        if ((this.ack_count > this.f + 1) && (this.active)) {
            System.out.println("deciding...");
            this.decide();
            this.active = false;
        }

        if ((this.nack_count > 0) && (this.ack_count + this.nack_count >= this.f + 1) && (this.active)) {
            System.out.println("Sending refined proposal");
            this.active_proposal_number += 1;
            this.ack_count = 0;
            this.nack_count = 0;
            this.beChannel.be_broadcast(new Proposal(active_proposal_number, (HashSet<Integer>) this.proposed_value.clone()));
        }
    }

    private void decide() {
        this.broadcaster.getApplicationLayer().log(this.proposed_value);
    }
}
