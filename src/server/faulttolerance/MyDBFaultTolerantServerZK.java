package server.faulttolerance;

/* CASSANDRA */
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;

/* CLIENT/SERVER MESSAGING */
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.AVDBReplicatedServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;

/* ZOOKEEPER */
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
// import com.example.DataMonitor;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.AsyncCallback;
import java.nio.charset.StandardCharsets;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single *fault-tolerant* Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;

	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS=true;

	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;

	public static final int DEFAULT_PORT = 2181;

    ZooKeeper zk;
	protected final String myID;
	String electionPath;
	String electionNode;
	boolean isLeader;
	String leaderID;
	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                      from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);

		// TODO: Make sure to do any needed crash recovery here.

		/* CONNECT TO SERVICES */
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect(myID);
		this.zk = new ZooKeeper("127.0.0.1" + ":" + Integer.toString(DEFAULT_PORT), 3000, null);

		this.myID = myID;
		this.isLeader = false;

		/* LEADER MANAGEMENT */
		try {
			zk.create("/election", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		catch(Exception e) {
			log.log(Level.INFO, "WARNING: exception during election parent creation, might be because it already exists: {0}", new Object[]{e});
		}
		try {
			/* ADD YOUR OWN ELECTION NODE TO ELECTION SEQUENCE */
			// each election node has corresponding server id
			this.electionPath = zk.create("/election/node", this.myID.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			String[] temp = electionPath.split("\\/");
			this.electionNode = temp[temp.length-1];

			ArrayList<String> nodes = new ArrayList<>(zk.getChildren("/election", false));
			Collections.sort(nodes);

			log.log(Level.INFO, "Server Zookeeper {0} has electionPath {1} and electionNode {2}", new Object[]{myID, electionPath, electionNode});
			for(String x: nodes) {
				log.log(Level.INFO, "Nodes in order is {0}", new Object[]{x});
			}

			/* FIND LEADER OR CHECK IF YOU ARE LEADER */
			int indexOfSelf = nodes.indexOf(this.electionNode);

			if(indexOfSelf == 0) {	// first in list, i am the leader!
				this.isLeader = true;
				this.leaderID = this.myID;
				log.log(Level.INFO, "Server Zookeeper {0} with electionPath {1} is now the supreme leader!", new Object[]{myID, electionPath});
			}

			else {	// i am a follower, i find leader and watch guy before me
				this.isLeader = false;
				// String watchNode = nodes.get(indexOfSelf-1);
				String watchNode = nodes.get(0);
				// String watchServerID = new String(zk.getData("/election/" + watchNode, false, null), StandardCharsets.UTF_8);
				this.leaderID = new String(zk.getData("/election/" + watchNode, false, null), StandardCharsets.UTF_8);

				// log.log(Level.INFO, "Server Zookeeper {0} with electionPath {1} now watching server {2} with electionNode {3}, and recognizes leader as {4}", 
				// 							new Object[]{myID, electionPath, watchServerID, watchNode, this.leaderID});
				log.log(Level.INFO, "Server Zookeeper {0} with electionPath {1} now watching leader {2} with electionNode {3}", 
											new Object[]{myID, electionPath, this.leaderID, watchNode});

				/* RECURSIVE WATCH FOR WHENEVER LEADER FAILS */
				zk.exists("/election/" + watchNode, new Watcher() {
					public void process(WatchedEvent event)
					{
						try{
							synchronized (this) {
								// if (event.getType() == Event.EventType.NodeDeleted) {	// a node got destroyed
									ArrayList<String> nodes = new ArrayList<>(zk.getChildren("/election", false));
									Collections.sort(nodes);
									int indexOfSelf = nodes.indexOf(electionNode);

									if (indexOfSelf == 0) {	// i am new leader
										isLeader = true;
										leaderID = myID;
										log.log(Level.INFO, "Server Zookeeper {0} with electionNode {1} is now the supreme leader!", new Object[]{leaderID, electionNode});
									}
									else {		// i watch new guy
										isLeader = false;
										String watchNode = nodes.get(0);
										// String watchServerID = new String(zk.getData("/election/" + watchNode, false, null), StandardCharsets.UTF_8);
										leaderID = new String(zk.getData("/election/" + watchNode, false, null), StandardCharsets.UTF_8);
										log.log(Level.INFO, "Server Zookeeper {0} with electionPath {1} now watching leader {2} with electionNode {3}", 
																	new Object[]{myID, electionPath, leaderID, watchNode});
										zk.exists("/election/" + watchNode, this);
									}
								// }
							}
						}
						catch(Exception e) {
							log.log(Level.SEVERE, "SEVERE WARNING: EXCEPTION OCCURRED DURING LEADER ELECTION WATCHER {0}", new Object[]{e});
						}
					}
				});
			}
			// String watchNode = nodes.get(
			// 	Collections.binarySearch(nodes, currentNode) - 1);
			// zk.exists("/election/" + watchNode, new Watcher() {
			// 	public void process(WatchedEvent event)
			// 	{
			// 		if (event.getType()
			// 			== Event.EventType.NodeDeleted) {
			// 			// Recheck if this node is now the leader
			// 		}
			// 	}
			// });
		}
		catch(Exception e){
			log.log(Level.SEVERE, "SEVERE WARNING: EXCEPTION OCCURRED DURING LEADER ELECTION {0}", new Object[]{e});
		}
		log.log(Level.INFO, "Server Zookeeper started with keyspace/myID {0}", new Object[]{myID});
	}

	/**
	 * TODO: process bytes received from clients here.
	 */
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		throw new RuntimeException("Not implemented");
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		throw new RuntimeException("Not implemented");
	}


	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	public void close() {
		throw new RuntimeException("Not implemented");
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
				.getInetSocketAddressFromString(args[2]) : new
				InetSocketAddress("localhost", 9042));
	}

}