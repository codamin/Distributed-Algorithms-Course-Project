package cs451.Primitives;

import cs451.Host;
import cs451.Primitives.Messages.Ack;
import cs451.Primitives.Messages.Nack;
import cs451.Primitives.Messages.Proposal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class Consensus {
    private HashMap<Integer, Integer> ack_count = new HashMap<>();
    private HashMap<Integer, Integer> nack_count = new HashMap<>();

    private HashMap<Integer, Boolean> active = new HashMap<>();
    private HashMap<Integer, HashSet<Integer>> accepted_values = new HashMap<>();
    private HashMap<Integer, Integer> round_last_proposal_number = new HashMap<>();
    private Integer active_proposal_number = 0;
    private Integer f;
    private Integer next_round = -1;
    private Host broadcaster;
    private BEChannel beChannel;
    private ArrayList<HashSet<Integer>> all_proposals;

//    private boolean lock = false;

    public Consensus(Host broadcaster, Integer NUMPROC, Integer NUMMSG, ArrayList<HashSet<Integer>> all_proposals) {
        this.broadcaster = broadcaster;
        this.all_proposals = all_proposals;

        this.f = (NUMPROC-1)/2;

        this.beChannel = new BEChannel(this.broadcaster.getHostsList(), this, this.broadcaster, NUMPROC, NUMMSG);
        this.beChannel.startThreads();
    }

    public void start() {
        this.propose_first_batch();
    }

    private void propose_first_batch() {
//        this.lock = true;
        for(int i = 0; i < 2; i++) {
            this.propose_next();
        }
//        this.lock = false;
    }

    private void propose_next() {
        this.next_round += 1;
        if(this.next_round > this.all_proposals.size()) {
            return;
        }
        if(this.next_round == this.all_proposals.size()) {
            System.out.println("finished");
            return;
        }

//        System.out.println("total: " + this.all_proposals.size() + " proposing round: " + this.next_round);
        this.accepted_values.computeIfAbsent(next_round, k -> new HashSet<>());
        this.active.put(this.next_round, true);

        this.active_proposal_number += 1;
        this.round_last_proposal_number.put(this.next_round, this.active_proposal_number);

        this.ack_count.put(this.next_round, 0);
        this.nack_count.put(this.next_round, 0);
        this.beChannel.be_broadcast(new Proposal(this.next_round, this.active_proposal_number, (HashSet<Integer>) this.all_proposals.get(this.next_round).clone()));
    }

    public void consensus_ack(Ack ack) {
//        System.out.println("rcvd ack " + ack.getProposal_number() + " --- " + "active_proposal_number: " + this.active_proposal_number);
        Integer ack_round = ack.getRound();
        if(ack.getProposal_number().equals(this.round_last_proposal_number.get(ack_round))) {
            this.ack_count.put(ack_round, this.ack_count.get(ack_round) + 1);
            this.handle_ack_nack(ack_round);
        }
    }

    public void consensus_nack(Nack nack) {
        Integer nack_round = nack.getRound();
        if(nack.getProposal_number().equals(this.round_last_proposal_number.get(nack_round))) {
            this.all_proposals.get(nack_round).addAll(nack.getAccepted_value());
            this.nack_count.put(nack_round, this.nack_count.get(nack_round) + 1);
            this.handle_ack_nack(nack_round);
        }
    }

    private void handle_ack_nack(Integer round) {
        if ((this.ack_count.get(round) >= this.f + 1) && this.active.get(round)) {
//            System.out.println("deciding...");
            this.active.put(round, false);
            this.decide(round);
        }

        if (this.active.get(round) && (this.ack_count.get(round) + this.nack_count.get(round) >= this.f + 1)) {
            this.active_proposal_number += 1;
            this.round_last_proposal_number.put(round, this.active_proposal_number);
            this.ack_count.put(round, 0);
            this.nack_count.put(round, 0);
            this.beChannel.be_broadcast(new Proposal(round, active_proposal_number, (HashSet<Integer>) this.all_proposals.get(round).clone()));
        }
    }

    public void get_proposal(String sourceIP, Integer sourcePort, Proposal proposal) {
        Integer proposal_round = proposal.getRound();
        HashSet<Integer> proposed_value = proposal.getProposed_value();
        this.accepted_values.computeIfAbsent(proposal_round, k -> new HashSet<>());
        if(proposed_value.containsAll(this.accepted_values.get(proposal_round))) {
            //send ack
            this.accepted_values.put(proposal_round, proposed_value);
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Ack(proposal_round, proposal.getProposal_number()));
        }
        else {
            //send nack
            this.accepted_values.get(proposal_round).addAll(proposed_value);
            this.beChannel.plChannel.pl_send(sourceIP, sourcePort, new Nack(proposal_round, proposal.getProposal_number(),
                    (HashSet<Integer>) this.accepted_values.get(proposal_round).clone()));
        }
    }

    private void decide(Integer decided_round) {
        this.broadcaster.getApplicationLayer().log(this.all_proposals.get(decided_round));
//        while(this.lock) {};
        this.propose_next();
    }
}
