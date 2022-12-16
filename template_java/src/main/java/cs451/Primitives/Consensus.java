package cs451.Primitives;

import cs451.Host;
import cs451.Primitives.Messages.Ack;
import cs451.Primitives.Messages.Decided;
import cs451.Primitives.Messages.Nack;
import cs451.Primitives.Messages.Proposal;

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

    private HashMap<Integer, Integer> round_active_proposal_number = new HashMap<>();

    private int last_round_decided = -1;

    private HashMap<Integer, HashSet> decisions = new HashMap<>();
    private Host broadcaster;
    private BEChannel beChannel;
//    private ArrayList<HashSet<Integer>> all_proposals;
    private HashMap<Integer, HashSet<Integer>> proposed_values = new HashMap<>();

    private HashMap<Integer, HashSet<Integer>> round_to_decided_procs = new HashMap<>();

    private int num_proc;

    public Consensus(Host broadcaster, Integer NUMPROC, int max_distinct_elems) {
        this.broadcaster = broadcaster;

        this.f = (NUMPROC-1)/2;
        this.num_proc = NUMPROC;

        this.beChannel = new BEChannel(this.broadcaster.getHostsList(), this, this.broadcaster, NUMPROC, max_distinct_elems);
        this.beChannel.startThreads();
    }

    public void start() {
        for(int i = 0; i < 10; i++) {
            this.propose_next();
        }
    }

    private synchronized void propose_next() {
        this.next_round += 1;
        this.active_proposal_number += 1;
        this.round_active_proposal_number.put(next_round, active_proposal_number);

        HashSet<Integer> new_proposal = this.broadcaster.readNextProposal();
        if(new_proposal == null) {
            System.out.println("finished");
            return;
        }
        this.proposed_values.put(this.next_round, new_proposal);
        this.active.put(this.next_round, true);
        this.ack_count.put(this.next_round, 0);
        this.nack_count.put(this.next_round, 0);
//        System.out.println(this.next_round);
        this.beChannel.be_broadcast(new Proposal(this.next_round, this.active_proposal_number, (HashSet<Integer>) this.proposed_values.get(this.next_round).clone()));
    }

    public void consensus_ack(Ack ack) {
        Integer ack_round = ack.getRound();
        if(ack.getProposal_number().equals(this.round_active_proposal_number.get(ack.getRound()))) {
//            System.out.println("ack_round: " + ack_round + " " + ack.getProposal_number());
            this.ack_count.put(ack_round, this.ack_count.get(ack_round) + 1);
            this.check_and_decide(ack_round);
//            this.check_refine_and_send(ack_round);
        }
    }

    public void consensus_nack(Nack nack) {
        Integer nack_round = nack.getRound();
        if(nack.getProposal_number().equals(this.round_active_proposal_number.get(nack.getRound()))) {
            if(! this.proposed_values.containsKey(nack_round)) {
              return;
            }
            this.proposed_values.get(nack_round).addAll(nack.getAccepted_value());
            this.nack_count.put(nack_round, this.nack_count.get(nack_round) + 1);
            this.check_refine_and_send(nack_round);
        }
    }

    private void check_refine_and_send(Integer round) {
//        if (this.active.get(round) && (this.ack_count.get(round) + this.nack_count.get(round) >= this.f + 1)) {
        if(this.active.get(round)) {
            this.active_proposal_number += 1;
            this.round_active_proposal_number.put(round, this.active_proposal_number);
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

        if(this.round_to_decided_procs.containsKey(proposal_round) && this.round_to_decided_procs.get(proposal_round).size() == this.num_proc) {
            return;
        }

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
        this.decisions.put(decided_round, (HashSet) this.proposed_values.get(decided_round).clone());
        while(this.decisions.containsKey(last_round_decided + 1)) {
            this.broadcaster.getApplicationLayer().log(this.decisions.get(last_round_decided + 1));
            this.beChannel.be_broadcast(new Decided(last_round_decided + 1));
            this.decisions.remove(last_round_decided + 1);
            this.last_round_decided += 1;
        }
    }

    public void announce_decided(Integer senderId, Decided msg) {
        this.round_to_decided_procs.computeIfAbsent(msg.getRound(), k -> new HashSet<>());
        this.round_to_decided_procs.get(msg.getRound()).add(senderId);
//        System.out.println(this.round_to_decided_procs.get(msg.getRound()).size() + " " + this.num_proc);
        if(this.round_to_decided_procs.get(msg.getRound()).size() == this.num_proc) {
//            System.out.println("gc ing!");
            this.accepted_values.remove(msg.getRound());
            this.proposed_values.remove(msg.getRound());
//            System.out.println(this.accepted_values.size() + " " + this.proposed_values.size() + " " + this.decisions.size());
        }
        if(senderId.equals(this.broadcaster.getId())) {
            if(this.round_to_decided_procs.get(msg.getRound()).size() >= this.f+1) {
                this.propose_next();
            }
        }
        else if(this.round_to_decided_procs.get(msg.getRound()).contains(this.broadcaster.getId()) &&
                this.round_to_decided_procs.get(msg.getRound()).size() == this.f+1) {
            this.propose_next();
        }
    }
}