package server.faulttolerance;

/* CASSANDRA */
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;

/* CLIENT/SERVER MESSAGING */
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
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
import org.apache.cassandra.cql3.Cql_Parser.mbean_return;
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
import java.util.LinkedList;

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
	int serverCount;
	// long lamport_clock = 0;
	long counter = 0;
	long expected_counter = 1;

	HashMap<String, Integer> ackMap = new HashMap<String, Integer>();; 	// counter|query -> number of acks
	LinkedList<String> queue = new LinkedList<String>();	// counter|query
	LinkedList<String> deliverQueue = new LinkedList<String>();	// counter|query

	boolean isLeader;
	String leaderID;

	String electionPath;
	String electionNode;

    protected final MessageNIOTransport<String,String> serverMessenger;

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
        // cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        // session = cluster.connect(myID);
		this.serverMessenger =  new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);

		this.zk = new ZooKeeper("127.0.0.1" + ":" + Integer.toString(DEFAULT_PORT), 3000, null);

		this.myID = myID;
		this.isLeader = false;
		this.serverCount = this.serverMessenger.getNodeConfig().getNodeIDs().size();

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
		}
		catch(Exception e){
			log.log(Level.SEVERE, "SEVERE WARNING: EXCEPTION OCCURRED DURING LEADER ELECTION {0}", new Object[]{e});
		}
		log.log(Level.INFO, "Server Zookeeper started with keyspace/myID {0}, detected server count is {1} with leader as {2}", new Object[]{myID, this.serverCount, this.leaderID});
	}

	/**
	 * TODO: process bytes received from clients here.
	 */
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        
		synchronized (this) {
			try{
				String request = new String(bytes, StandardCharsets.UTF_8);

				// forward the request to the leader as a proposal   
				String proposal = "PROPOSAL|" +  request;
				this.serverMessenger.send(this.leaderID, proposal.getBytes(StandardCharsets.UTF_8)); 

				log.log(Level.INFO, "Server Zookeeper {0} received client message {1} from {2}, sending {3} to {4}",
						new Object[]{this.myID, request, header.sndr, proposal, this.leaderID});

			} catch (Exception e) {
				log.log(Level.SEVERE, "SEVERE WARNING: EXCEPTION OCCURRED FOR {0} IN handleMessageFromClient {1}", new Object[]{this.myID, e});
			}	
		}
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		synchronized (this) {
			try{
				/* message should be opcode|query for proposal and opcode|query|requestID for every other opcode */
				String message = new String(bytes, StandardCharsets.UTF_8);
				String[] message_parts = message.split("\\|");

				if(message_parts.length <= 1) { // use only messages containing | as that is how I format messages
					return;
				}

				log.log(Level.INFO, "Server Zookeeper {0} received server message {1} from {2}", new Object[]{this.myID, message, header.sndr});

				if(message_parts[0].equals("PROPOSAL")) {		// only leaders get proposals
					if(this.isLeader) {
						/* BEGIN MULTICAST */
						this.counter = this.counter+1;
						String messageToBroadcast = "PREPARE|" + message_parts[1] + "|" + this.counter;
						this.queue.add("" + this.counter + "|" + message_parts[1]);
						for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
							this.serverMessenger.send(node, messageToBroadcast.getBytes(StandardCharsets.UTF_8));
						}
					}
					else {
						log.log(Level.SEVERE, "SEVERE WARNING: SERVER ZOOKEEPER {0} RECEIVED {1} BUT {2} IS LEADER", new Object[]{this.myID, message, this.leaderID});
					}
				}
				else if(message_parts[0].equals("PREPARE")) {		// all nodes must respond with prepareack
					String messageToRespond = "PREPAREACK|" + message_parts[1] + "|" + message_parts[2];;
					this.serverMessenger.send(this.leaderID, messageToRespond.getBytes(StandardCharsets.UTF_8));
					// log.log(Level.INFO, "Server Zookeeper {0} sends message {1} to {2}", new Object[]{this.myID, messageToRespond, this.leaderID});
				}
				else if(message_parts[0].equals("PREPAREACK")) {	// leader send DECISION if majority

					int numServers = this.serverMessenger.getNodeConfig().getNodeIDs().size();
					if(this.serverCount != numServers) {
						log.log(Level.SEVERE, "SEVERE WARNING: SERVER ZOOKEEPER {0} this.serverCount={1} but actual number is {2}", new Object[]{this.myID, this.serverCount, numServers});
					}

					String key = "" + message_parts[2] + "|" + message_parts[1];
					if(this.ackMap.containsKey(key)) {
						this.ackMap.put(key, 1 + this.ackMap.get(key));
					}
					else {
						this.ackMap.put(key, 1);
					}

					/* FLUSH QUEUE FOR DECISIONS */
					while(!this.queue.isEmpty()) {
						String front_message = this.queue.peek();
						String[] front_message_parts = front_message.split("\\|");

						if(!this.ackMap.containsKey(front_message)) {    // means not ready yet
							log.log(Level.INFO, "ackMap does not contain {0} yet, returning", new Object[]{front_message});
							return;
						}
						int acks = this.ackMap.get(front_message);
						log.log(Level.INFO, "front of queue is {0} with {1} acks", new Object[]{front_message, acks});

						if(acks > this.serverCount/2) {		// > or >= for majority?
							/* MULTICAST DECISION */
							String messageToBroadcast = "DECISION|" + front_message_parts[1] + "|" + front_message_parts[0];
							for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
								this.serverMessenger.send(node, messageToBroadcast.getBytes(StandardCharsets.UTF_8));
							}
							this.ackMap.remove(front_message);
							this.queue.poll();
							log.log(Level.INFO, "Server Zookeeper {0} popped {1} from front of queue, messageToBroadcast={2}", new Object[]{this.myID, front_message, messageToBroadcast});
						}
						else {
							break;
						}
					}
				}
				else if(message_parts[0].equals("DECISION")) {	// commit operation once its in order

					this.deliverQueue.add("" + message_parts[2] + "|" + message_parts[1]);
                    this.deliverQueue.sort((s1, s2) -> {
                        String[] s1_parts = s1.split("\\|");	// [counter, request]
                        String[] s2_parts = s2.split("\\|");

                        return s1_parts[0].compareTo(s2_parts[0]);
                    });

					/* FLUSH COMMIT OPERATIONS */
					while(!this.deliverQueue.isEmpty()) {
						String front_message = this.deliverQueue.peek();
						String[] front_message_parts = front_message.split("\\|");	

						long front_counter = Long.parseLong(front_message_parts[0]);
						if(front_counter <= this.expected_counter) {
							log.log(Level.INFO, "Server Zookeeper {0} expected counter {1}, delivering counter {2} and message {3}", new Object[]{this.myID, this.expected_counter, front_counter, front_message_parts[1]});
							this.session.execute(front_message_parts[1]);
							this.expected_counter = front_counter+1;
							this.deliverQueue.poll();
						}
						else {
							break;
						}
					}
				}

			} catch (Exception e) {
				log.log(Level.SEVERE, "SEVERE WARNING: EXCEPTION OCCURRED FOR {0} IN handleMessageFromServer {1}", new Object[]{this.myID, e});
			}	
		}	
	}


	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	public void close() {
		super.close();
	    this.serverMessenger.stop();
	    session.close();
	    cluster.close();
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