package cs451.Primitives;

import cs451.Host;
import cs451.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class URBChannel {

    private Host broadcaster;
    private List<Host> hostsList;
    private BEChannel beChannel;

    private FIFOChannel upperChannel;
    public URBChannel(List<Host> hostsList, FIFOChannel fifoChannel, Host broadcaster) {
        this.hostsList = hostsList;
        this.beChannel = new BEChannel(this.hostsList, this, broadcaster);
        this.upperChannel = fifoChannel;
        this.broadcaster = broadcaster;
    }

    HashSet<Integer> urb_deliveredSet = new HashSet<>();
    HashMap<Integer, ArrayList<Message>> urb_pendingMap = new HashMap<>();
    HashMap<Integer, HashSet<Integer>> urb_ackedMap = new HashMap<>();

    public void urb_broadcast(Integer broadcasterId, Message msg) {
        System.out.println("urb broadcast...");
        // put the msg in broadcaster (as the original sender) pending list
        System.out.println(broadcasterId);
        this.urb_pendingMap.put(broadcasterId, new ArrayList<>() {{add(msg);}});
        this.beChannel.be_broadcast(msg);
    }

    public void urb_deliver(Integer beDelivererId, Message msg) {

        //************ ack[m] := ack[m] ∪ {p}; **************
        ///////////////////////////////////////////
        System.out.println("in urb delivery...");

        if(urb_ackedMap.get(msg.getSeqNumber()) != null) {
            urb_ackedMap.get(msg.getSeqNumber()).add(beDelivererId);
        }
        else {
            urb_ackedMap.put(msg.getSeqNumber(), new HashSet<>(){{add(beDelivererId);}});
        }
        ///////////////////////////////////////////
        //************ if (s, m) is not pending then
        // pending := pending ∪ {(s, m)};
        // trigger < beb, Broadcast | [DATA, s, m] >; **************
        ///////////////////////////////////////////
        System.out.println(urb_pendingMap.get(broadcaster.getId()).get(0).getSeqNumber());
        System.out.println("1 " + msg.getSeqNumber());
        System.out.println("2 " + msg.getMsgContent());
        System.out.println("3 " + msg.getOriginalSenderId());

        System.out.println("#");
        System.out.println("1 " + urb_pendingMap.get(broadcaster.getId()).get(0).getSeqNumber());
        System.out.println("2 " + urb_pendingMap.get(broadcaster.getId()).get(0).getMsgContent());
        System.out.println("3 " + urb_pendingMap.get(broadcaster.getId()).get(0).getOriginalSenderId());

        System.out.println(msg.equals(urb_pendingMap.get(broadcaster.getId()).get(0)));
        Integer s = msg.getOriginalSenderId();
        if( urb_pendingMap.get(s) == null) {
            System.out.println("msg was not in the pending list");
            urb_pendingMap.put(s, new ArrayList(){{add(msg);}});
            System.out.println("relaying msg with s: " + msg.getOriginalSenderId() + " and sn: " + msg.getSeqNumber());
            // relay message
            beChannel.be_broadcast(msg);
            // deliver if can deliver
            System.out.println("calling checkAndDeliverToFiFo:");
            checkAndDeliverToFiFo(s, msg);
        }
        else if(! urb_pendingMap.get(s).contains(msg)) {
            System.out.println("it did not contain");
            urb_pendingMap.get(s).add(msg);
            // relay message
            beChannel.be_broadcast(msg);
            // deliver if can deliver
            checkAndDeliverToFiFo(s, msg);
        }
    }

    private void checkAndDeliverToFiFo(Integer s, Message msg) {
        if(urb_ackedMap.get(msg.getSeqNumber()).size() > (this.hostsList.size()+1)/2) {
            if(! urb_deliveredSet.contains(msg.getSeqNumber())) {
                urb_deliveredSet.add(msg.getSeqNumber());
                upperChannel.fifo_deliver(msg);
            }
        }
    }
}
