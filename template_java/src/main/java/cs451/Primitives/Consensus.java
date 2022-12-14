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
    private Integer active_proposal_number = 0;
    private Integer f;
    private Integer next_round = -1;
    private Host broadcaster;
    private BEChannel beChannel;
//    private ArrayList<HashSet<Integer>> all_proposals;
    private HashMap<Integer, HashSet<Integer>> proposed_values = new HashMap<>();

    public Consensus(Host broadcaster, Integer NUMPROC) {
        this.broadcaster = broadcaster;

        this.f = (NUMPROC-1)/2;

        this.beChannel = new BEChannel(this.broadcaster.getHostsList(), this, this.broadcaster);
        this.beChannel.startThreads();
    }

    public void start() {
        this.propose_first_batch();
    }

    private void propose_first_batch() {
//        for(int i = 0; i < 10; i++) {
        this.propose_next();
//        }
    }

    private void propose_next() {
        this.next_round += 1;

        HashSet<Integer> new_proposal = this.broadcaster.readNextProposal();
        if(new_proposal == null) {
            System.out.println("finished");
            return;
        }
        this.proposed_values.put(this.next_round, new_proposal);
        this.active.put(this.next_round, true);
        this.active_proposal_number += 1;
        this.ack_count.put(this.next_round, 0);
        this.nack_count.put(this.next_round, 0);
        this.beChannel.be_broadcast(new Proposal(this.next_round, this.active_proposal_number, (HashSet<Integer>) this.proposed_values.get(this.next_round).clone()));
    }

    public void consensus_ack(Ack ack) {
        Integer ack_round = ack.getRound();
        if(ack.getProposal_number().equals(active_proposal_number)) {
            this.ack_count.put(ack_round, this.ack_count.get(ack_round) + 1);
            this.check_and_decide(ack_round);
//            this.check_refine_and_send(ack_round);
        }
    }

    public void consensus_nack(Nack nack) {
        Integer nack_round = nack.getRound();
        if(nack.getProposal_number().equals(active_proposal_number)) {
            this.proposed_values.get(nack_round).addAll(nack.getAccepted_value());
            this.nack_count.put(nack_round, this.nack_count.get(nack_round) + 1);
            this.check_refine_and_send(nack_round);
        }
    }

    private void check_refine_and_send(Integer round) {
//        if (this.active.get(round) && (this.ack_count.get(round) + this.nack_count.get(round) >= this.f + 1)) {
        if(this.active.get(round)) {
            this.active_proposal_number += 1;
//            this.round_last_proposal_number.put(round, this.active_proposal_number);
            this.ack_count.put(round, 0);
            this.nack_count.put(round, 0);
            this.beChannel.be_broadcast(new Proposal(round, active_proposal_number, (HashSet<Integer>) this.proposed_values.get(round).clone()));
        }
//        }
    }

    private void check_and_decide(Integer round) {
        if ((this.ack_count.get(round) >= this.f + 1) && this.active.get(round)) {
//            System.out.println("deciding...");
            this.active.put(round, false);
            this.decide(round);
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
//        if(decided_round == this.last_round_decided + 1) {
//            this.broadcaster.getApplicationLayer().log(this.accepted_values.get(decided_round));
//            this.last_round_decided += 1;
//            while(!this.active.get(last_round_decided)) {
//                this.broadcaster.getApplicationLayer().log(this.accepted_values.get(last_round_decided));
//                this.last_round_decided += 1;
//            }
//            this.propose_next();
//        }
        this.broadcaster.getApplicationLayer().log(this.proposed_values.get(decided_round));
        this.propose_next();
    }
}
