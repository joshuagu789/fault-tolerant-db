package client;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.SingleServer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.util.HashMap;

/**
 * This class should implement your DB client.
 */
public class MyDBClient extends Client {
    private NodeConfig<String> nodeConfig= null;
    private HashMap<Long, Callback> callbacks = new HashMap<Long, Callback>();
    private long callback_id = 0;

    public MyDBClient() throws IOException {
    }
    public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
        super();
        this.nodeConfig = nodeConfig;
    }

    /** TODO: This method will automatically get invoked whenever any response
     * is received from a remote node. You need to implement logic here to
     * ensure that whenever a response is received, the callback method
     * that was supplied in callbackSend(.) when the corresponding request
     * was sent is invoked.
     *
     * Extend this method in MyDBClient to implement your logic there. This
     * file will be overwritten to the original in your submission.
     *
     * @param bytes The content of the received response
     * @param header Sender and receiver information about the received response
     */
    @Override
    protected void handleResponse(byte[] bytes, NIOHeader header) {
        // expect echo reply by default here
        try {
            String response = new String(bytes, SingleServer.DEFAULT_ENCODING);
            // System.out.println("handleResponse received string " + response);

            for(int i = response.length()-1; i >= 0; i--) {

                if(response.charAt(i) == '|') { // if callback exists, it will be to right of '|'

                    long id = Long.parseLong(response.substring(i+1));
                    String message = response.substring(0, i);
                    // System.out.println("| detected at index " + Integer.toString(i)
                    //                     + " for message " + response + " and id " + Long.toString(id));

                    synchronized (this) {
                        if(this.callbacks.containsKey(id)) {
                            Callback callback = this.callbacks.get(id);
                            // callback.handleResponse(bytes, header);
                            callback.handleResponse(message.getBytes(SingleServer.DEFAULT_ENCODING), header);
                            this.callbacks.remove(id);
                        }
                    }

                    break;
                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: This method, unlike the simple send above, should invoke the
     * supplied callback's handleResponse method upon receiving the
     * corresponding response from the remote end.
     *
     * Extend this method in MyDBClient to implement your logic there. This
     * file will be overwritten to the original in your submission.
     *
     * @param isa
     * @param request
     * @param callback
     */
    @Override
    public void callbackSend(InetSocketAddress isa, String request, Callback
            callback) throws IOException {

        String payload;

        // this.callback_id is critical section
        synchronized (this) {
            if(callback != null) {
                this.callbacks.put(this.callback_id, callback);
            }
            payload = request + "|" + Long.toString(callback_id);   // append callback id with '|' delimiter
            this.callback_id = this.callback_id+1;                                     // for convenience, always increment & send callback id even if no callback
        }
        System.out.println("Sending callbacked string in callbackSend " + payload);
        this.send(isa, payload);
    }
}
// package client;

// import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
// import edu.umass.cs.nio.MessageNIOTransport;
// import edu.umass.cs.nio.interfaces.NodeConfig;
// import edu.umass.cs.nio.nioutils.NIOHeader;
// import server.SingleServer;

// import java.io.IOException;
// import java.io.UnsupportedEncodingException;
// import java.net.InetSocketAddress;

// /**
//  * This class should implement your DB client.
//  */
// public class MyDBClient extends Client {
//     private NodeConfig<String> nodeConfig= null;
//     public MyDBClient() throws IOException {
//     }
//     public MyDBClient(NodeConfig<String> nodeConfig) throws IOException {
//         super();
//         this.nodeConfig = nodeConfig;
//     }
// }