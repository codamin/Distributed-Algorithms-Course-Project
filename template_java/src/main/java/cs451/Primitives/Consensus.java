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
    private Integer ack_count;
    private Integer nack_count;
//    private HashMap<Integer, HashSet<Integer>> accepted_values = new HashMap<>();
    private HashMap<Integer, Integer> round_starting_proposal_number = new HashMap<>();
    private Integer active_proposal_number;
    private Integer f;
    private Integer round = -1;
    private Host broadcaster;
    private BEChannel beChannel;
    private ArrayList<HashSet<Integer>> all_proposals;

    private boolean active;

    public Consensus(Host broadcaster, Integer NUMPROC, Integer NUMMSG, ArrayList<HashSet<Integer>> all_proposals) {
        this.broadcaster = broadcaster;
        this.all_proposals = all_proposals;

        this.active = false;
        this.ack_count = 0;
        this.nack_count = 0;
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
            this.round = this.all_proposals.size() - 1;
            System.out.println("finished");
            return;
        }
        System.out.println("total: " + this.all_proposals.size() + " proposing round: " + this.round);
//        this.accepted_values.computeIfAbsent(round, k -> new HashSet<>());
        this.active = true;

        this.active_proposal_number += 1;
        this.round_starting_proposal_number.put(this.round, this.active_proposal_number);

        this.ack_count = 0;
        this.nack_count = 0;
        this.beChannel.be_broadcast(new Proposal(this.round, this.active_proposal_number, (HashSet<Integer>) this.all_proposals.get(this.round).clone()));
    }

    public void consensus_ack(Ack ack) {
//        System.out.println("rcvd ack " + ack.getProposal_number() + " --- " + "active_proposal_number: " + this.active_proposal_number);
        if(ack.getProposal_number().equals(this.active_proposal_number)) {
            this.ack_count += 1;

//            this.handle_rcvd_ack_nack();
            if ((this.ack_count >= this.f + 1) && this.active) {
//            System.out.println("deciding...");
                this.active = false;
                this.decide();
            }
        }
    }

    public void consensus_nack(Nack nack) {
        if(nack.getProposal_number().equals(this.active_proposal_number)) {
            this.all_proposals.get(this.round).addAll(nack.getAccepted_value());
            this.nack_count += 1;
//            this.handle_rcvd_ack_nack();
            this.active_proposal_number += 1;
            this.ack_count = 0;
            this.nack_count = 0;
            this.beChannel.be_broadcast(new Proposal(this.round, active_proposal_number, (HashSet<Integer>) this.all_proposals.get(round).clone()));
        }
    }

    public void get_proposal(String sourceIP, Integer sourcePort, Proposal proposal) {
//        this.accepted_values.computeIfAbsent(proposal.getCorresponding_round(), k -> new HashSet<>());
        if(proposal.getProposed_value().containsAll(this.all_proposals.get(proposal.getCorresponding_round()))) {
            //send ack
            this.all_proposals.set(proposal.getCorresponding_round(), proposal.getProposed_value());
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Ack(proposal.getProposal_number()));
        }
        else {
            //send nack
            this.all_proposals.get(proposal.getCorresponding_round()).addAll(proposal.getProposed_value());
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Nack(proposal.getProposal_number(),
                    (HashSet<Integer>) this.all_proposals.get(proposal.getCorresponding_round()).clone()));
        }
    }

//    public void handle_rcvd_ack_nack() {
////        if ((this.nack_count > 0) && (this.ack_count + this.nack_count >= this.f + 1)) {
////            this.active_proposal_number += 1;
////            this.ack_count = 0;
////            this.nack_count = 0;
////            this.beChannel.be_broadcast(new Proposal(this.round, active_proposal_number, (HashSet<Integer>) this.all_proposals.get(round).clone()));
////        }
//
////        if ((this.ack_count >= this.f + 1)) {
//////            System.out.println("deciding...");
//////            this.active = false;
////            this.decide();
//        }
//    }

    private void decide() {
        this.broadcaster.getApplicationLayer().log(this.all_proposals.get(this.round));
        this.propose_next();
    }
}
