package cs451.Primitives;

import cs451.Host;
import cs451.Primitives.Messages.*;

import java.util.HashMap;
import java.util.List;

public class BEChannel {

    private Host broadcaster;
    private List<Host> hostsList;
    public PLChannel plChannel;
    public Consensus consensus;

    private HashMap<String, HashMap<Integer, Integer>> host2IdMap;

    public BEChannel(List<Host> hostsList, Consensus consensus, Host broadcaster, int NUM_PROC, int max_distinct_elems) {
        this.hostsList = hostsList;
        this.broadcaster = broadcaster;
        // creating a host2IdMap to send to pl channel. The pl channel will need it to know the id of the sender of a msg
        // so that it can create dictionaries easier. Anyways, even if pl channel did not do this mapping, the be channel
        // eventually would need to do it.
        ////////////////////////
        HashMap<String, HashMap<Integer, Integer>> host2IdMap = new HashMap<>();
        for(Host host: hostsList) {
            if(host2IdMap.get(host.getIp()) == null) {
                host2IdMap.put(host.getIp(), new HashMap<>() {{put(host.getPort(), host.getId());}});
            }
            else {
                host2IdMap.get(host.getIp()).put(host.getPort(), host.getId());
            }
        }
        this.host2IdMap = host2IdMap;
        ////////////////////////
        this.consensus = consensus;
        this.plChannel = new PLChannel(this, broadcaster, host2IdMap, NUM_PROC, max_distinct_elems);
    }

    public void startThreads() {
        this.plChannel.startThreads();
    }

    public void be_broadcast(Message message) {
        // do a for loop
        for(Host host: this.hostsList) {
//            System.out.println("broadcasting msg:" + fifoMsg);
            plChannel.pl_send(host.getIp(), host.getPort(), message);
        }
    }

    public void be_deliver(String sourceIp, Integer sourcePort, Message msg) {
        if(msg instanceof Proposal) {
//            System.out.println("be delivering a proposal");
            consensus.get_proposal(sourceIp, sourcePort, (Proposal) msg);
        }
        else if(msg instanceof Ack) {
//            System.out.println("be delivering an ack");
            consensus.consensus_ack((Ack) msg);
        }
        else if(msg instanceof Nack) {
//            System.out.println("be delivering a nack");
            consensus.consensus_nack((Nack) msg);
        }
        else if(msg instanceof Decided) {
//            System.out.println(host2IdMap.get(sourceIp).get(sourcePort) + " has decided on round: " + msg.getRound());
            consensus.announce_decided(host2IdMap.get(sourceIp).get(sourcePort), (Decided) msg);
        }
    }
}