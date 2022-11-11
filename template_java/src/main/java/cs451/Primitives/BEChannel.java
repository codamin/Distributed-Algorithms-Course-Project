package cs451.Primitives;

import cs451.FIFOMessage;
import cs451.Host;
import cs451.Message;

import java.util.HashMap;
import java.util.List;

public class BEChannel {

    private Host broadcaster;
    private List<Host> hostsList;

    private URBChannel upperChannel;
    private PLChannel plChannel;
    public BEChannel(List<Host> hostsList, URBChannel urbChannel, Host broadcaster) {
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

        ////////////////////////
        this.plChannel = new PLChannel(this, broadcaster, host2IdMap);
        this.upperChannel = urbChannel;


    }

    public void be_broadcast(FIFOMessage fifoMsg) {

        // do a for loop
        for(Host host: this.hostsList) {
            System.out.println("be broadcast to host: " + host.getId());
            System.out.println("broadcasting msg:" + fifoMsg);
            plChannel.pl_send(host.getIp(), host.getPort(), broadcaster.getId(), fifoMsg);
        }
        System.out.println("finished be broadcasting%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
    }

    public void be_deliver(Integer senderId, FIFOMessage msg) {
        // deliver : call the delivery function of urb
        System.out.println("in be delivery...");
        upperChannel.urb_deliver(senderId, msg);
    }
}