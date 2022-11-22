package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.Host;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class URBChannel {

    private Host broadcaster;
    private List<Host> hostsList;
    private BEChannel beChannel;

    private FIFOChannel upperChannel;

    private HashSet<FIFOMessage> urb_deliveredSet = new HashSet<>();
    private HashSet<FIFOMessage> urb_pendingSet = new HashSet<>();
    private HashMap<FIFOMessage, HashSet<Integer>> urb_ackedMap = new HashMap<>();

    public URBChannel(List<Host> hostsList, FIFOChannel fifoChannel, Host broadcaster) {
        this.hostsList = hostsList;
        this.beChannel = new BEChannel(this.hostsList, this, broadcaster);
        this.upperChannel = fifoChannel;
        this.broadcaster = broadcaster;
    }

    public void urb_broadcast(Integer broadcasterId, FIFOMessage msg) {
//        System.out.println("urb broadcast...");
        // put the msg in broadcaster (as the original sender) pending list
        this.urb_pendingSet.add(msg);
        this.beChannel.be_broadcast(msg);
    }

    public void urb_deliver(Integer senderId, FIFOMessage msg) {

        //************ ack[m] := ack[m] ∪ {p}; **************
        ///////////////////////////////////////////
        System.out.println("in urb delivery:");
//        System.out.println("from sender: " + senderId + " received msg:");
//        System.out.println(msg);

        if(urb_ackedMap.get(msg) != null) {
            urb_ackedMap.get(msg).add(senderId);
            urb_ackedMap.get(msg).add(broadcaster.getId());
        }
        else {
            urb_ackedMap.put(msg, new HashSet<>(){{add(senderId); add(broadcaster.getId());
            }});
        }

//        System.out.println("urb_ackedMap:");
//        System.out.println(urb_ackedMap);

        ///////////////////////////////////////////
        //if (s, m) is not pending then
        // pending := pending ∪ {(s, m)};
        // trigger < beb, Broadcast | [DATA, s, m] >;
        ///////////////////////////////////////////
        if(! urb_pendingSet.contains(msg)) {
            urb_pendingSet.add(msg);
             //relay message
//            beChannel.be_broadcast(msg, new HashSet<>() {{add(senderId); add(broadcaster.getId());}});
            System.out.println("relaying msg:"  + msg);
            beChannel.be_broadcast(msg);
//           // deliver if can deliver
        }
        checkAndDeliverToFiFo(msg);
    }

    private void checkAndDeliverToFiFo(FIFOMessage msg) {
        if(urb_ackedMap.get(msg).size() > (this.hostsList.size()/2)) {
            if(! urb_deliveredSet.contains(msg)) {
                urb_deliveredSet.add(msg);
                upperChannel.fifo_deliver(msg);
            }
        }
    }
}
