package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {

    protected Cluster cluster;
    protected Session session;
    
    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);

        cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).build();
        session = cluster.connect(keyspace);
    }

    public void close() {
        session.close();
        cluster.close();
        super.close();
        // TODO: cleanly close anything you created here.
    }

    /**
     *    TODO: process request bytes received from clients here by relaying
     *    them to the database server. The default below simply echoes back
     *    the request.
     *
     *    Extend this method in MySingleServer to implement your logic there.
     *    This file will be overwritten to the original in your submission.
      */
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // simple echo server
        try {

            String message = "";
            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
            String[] request_parts = request.split("\\|");  // contains <cql command>|<callback_id> or just <cql command>

            // System.out.println("handleMessageFromClient received string " + request);

            synchronized (this) {

                // System.out.println("handleMessageFromClient executing string " + request_parts[0]);
                this.session.execute(request_parts[0]);

                this.clientMessenger.send(header.sndr, bytes);  // echo message
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
// package server;

// import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
// import edu.umass.cs.nio.MessageNIOTransport;
// import edu.umass.cs.nio.nioutils.NIOHeader;

// import java.io.IOException;
// import java.net.InetSocketAddress;
// import java.util.logging.Level;
// import java.util.logging.Logger;

// /**
//  * This class should implement the logic necessary to perform the requested
//  * operation on the database and return the response back to the client.
//  */
// public class MyDBSingleServer extends SingleServer {
//     public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
//                             String keyspace) throws IOException {
//         super(isa, isaDB, keyspace);
//     }
// }