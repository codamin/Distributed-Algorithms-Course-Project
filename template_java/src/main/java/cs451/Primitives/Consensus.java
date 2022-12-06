package cs451.Primitives;

import cs451.Host;
import cs451.Primitives.Messages.Ack;
import cs451.Primitives.Messages.Nack;
import cs451.Primitives.Messages.Proposal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Consensus {
//    boolean active;
    private HashMap<Integer, Boolean> actives = new HashMap<>();
//    private Integer ack_count;
    private HashMap<Integer, Integer> ack_counts = new HashMap<>();
//    private Integer nack_count;
    private HashMap<Integer, Integer> nack_counts = new HashMap<>();

    private HashMap<Integer, HashSet<Integer>> accepted_values = new HashMap<>();
//    private HashSet<Integer> proposed_value;
//    private HashMap<Integer, HashSet<Integer>> proposed_values = new HashMap<>();

    private HashMap<Integer, Integer> round_starting_proposal_number = new HashMap<>();
    private Integer active_proposal_number;
    private Integer f;
    private Integer round = -1;
    private Host broadcaster;
    private BEChannel beChannel;

    private ArrayList<HashSet<Integer>> all_proposals;

    public Consensus(Host broadcaster, Integer NUMPROC, Integer NUMMSG, ArrayList<HashSet<Integer>> all_proposals) {
        this.broadcaster = broadcaster;
        this.all_proposals = all_proposals;

        this.actives.put(round, false);
        this.ack_counts.put(round, 0);
        this.nack_counts.put(round, 0);
        this.active_proposal_number = 0;
        this.f = (NUMPROC-1)/2;

        this.beChannel = new BEChannel(this.broadcaster.getHostsList(), this, this.broadcaster, NUMPROC, NUMMSG);
        this.beChannel.startThreads();
    }

    public void start() {
        this.propose_next();
    }

    public void propose_next() {
        this.round += 1;
        if(this.round > this.all_proposals.size() - 1) {
            return;
        }
        this.accepted_values.computeIfAbsent(round, k -> new HashSet<>());

//        this.proposed_values.put(round, proposal);
        this.actives.put(round, true);

        this.active_proposal_number += 1;
        this.round_starting_proposal_number.put(this.round, this.active_proposal_number);

        this.ack_counts.put(round, 0);
        this.nack_counts.put(round, 0);
        this.beChannel.be_broadcast(new Proposal(this.round, this.active_proposal_number, (HashSet<Integer>) this.all_proposals.get(this.round).clone()));
    }

    public void consensus_ack(Ack ack) {
//        System.out.println("rcvd ack for proposal: " + ack.getProposal_number() + " active_proposal_number = " + this.active_proposal_number);
        if(ack.getProposal_number() == this.active_proposal_number) {
            this.ack_counts.put(this.round, this.ack_counts.get(round) + 1);

            this.handle_rcvd_ack_nack();
        }
    }

    public void consensus_nack(Nack nack) {
        System.out.println("rcvd nack for proposal: " + nack.getProposal_number() + " active_proposal_number = " + this.active_proposal_number);
        if(nack.getProposal_number() == this.active_proposal_number) {
            this.all_proposals.get(this.round).addAll(nack.getAccepted_value());
            this.nack_counts.put(this.round, this.nack_counts.get(this.round) + 1);

            this.handle_rcvd_ack_nack();
        }
    }

    public void get_proposal(String sourceIP, Integer sourcePort, Proposal proposal) {
        System.out.println("rcvd proposal with number: " + proposal.getProposal_number());
        this.accepted_values.computeIfAbsent(proposal.getCorresponding_round(), k -> new HashSet<>());
        if(proposal.getProposed_value().containsAll(this.accepted_values.get(proposal.getCorresponding_round()))) {
            //send ack
            this.accepted_values.put(proposal.getCorresponding_round(), proposal.getProposed_value());
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Ack(proposal.getProposal_number()));
        }
        else {
            //send nack
            this.accepted_values.get(proposal.getCorresponding_round()).addAll(proposal.getProposed_value());
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Nack(proposal.getProposal_number(),
                    (HashSet<Integer>) this.accepted_values.get(proposal.getCorresponding_round()).clone()));
        }
    }

    public void handle_rcvd_ack_nack() {
//        System.out.println("handling rcvd ack/nack:");
//        System.out.println("active: " + this.actives.get(round));
//        System.out.println("ack_count: " + this.ack_counts.get(round));
//        System.out.println("nack_count: " + this.nack_counts.get(round))
        if ((this.ack_counts.get(round) != null) && (this.ack_counts.get(round) > this.f + 1) && (this.actives.get(round))) {
            System.out.println("deciding...");
            this.actives.put(round, false);
            this.decide();
        }

        if ((this.nack_counts.get(round) != null) && (this.ack_counts.get(round) != null) && (this.nack_counts.get(round) > 0) && (this.ack_counts.get(round) +
                this.nack_counts.get(round) >= this.f + 1) && (this.actives.get(round))) {
            this.active_proposal_number += 1;
            this.ack_counts.put(round, 0);
            this.nack_counts.put(round, 0);
            this.beChannel.be_broadcast(new Proposal(this.round, active_proposal_number, (HashSet<Integer>) this.all_proposals.get(round).clone()));
        }
    }

    private void decide() {
        this.broadcaster.getApplicationLayer().log(this.accepted_values.get(round));
//        this.broadcaster.sendNextBatch();
        this.propose_next();
        System.out.println("sending next");
    }
}
