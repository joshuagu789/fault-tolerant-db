package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

    protected Cluster cluster;
    protected Session session;

	protected static final Logger log = Logger.getLogger(MyDBReplicableAppGP.class.getName());

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect(args[0]);
		// throw new RuntimeException("Not yet implemented");
		// System.out.println("Replicable Giga Paxos started with keyspace " + args[0]);

		for(int i = 0; i < args.length; i++) {
			log.log(Level.INFO, "{0}", new Object[]{args[i]});
		}

		log.log(Level.INFO, "Replicable Giga Paxos started with keyspace {0}", new Object[]{args[0]});
	}

	/**
	 * This method must handle the request atomically and return true if
	 * successful or throw an exception or return false. It is the application's
	 * responsibility to ensure atomicity, i.e., it should return true iff the
	 * request was successfully executed; if it returns false or throws an
	 * exception, the application should ensure that the state of the
	 * application rolls back any partial execution of this request. If it
	 * returns false or throws an exception, the replica coordination protocol
	 * may try to re-attempt executing the request.
	 * 
	 * @param request
	 * @param doNotReplyToClient
	 *            If true, the application is expected to not send a response
	 *            back to the originating client (say, because this request is
	 *            part of a post-crash roll-forward or only the "entry replica"
	 *            that received the request from the client is expected to
	 *            respond back. If false, the application is expected to either
	 *            send a response (if any) back to the client via the
	 *            {@link ClientMessenger} interface or delegate response
	 *            messaging to paxos via the {@link ClientRequest#getResponse()}
	 *            interface.
	 * 
	 * @return Must return true if and only if the application handled the
	 *         request successfully. For safety, executing a request must have a
	 *         deterministic effect on the safety-critical state. If the request
	 *         is bad and is to be discarded, the application must still return
	 *         true (after "successfully" discarding it).
	 * 
	 *         If the application returns false, the replica coordination
	 *         protocol (e.g., paxos) might try to repeatedly re-execute it
	 *         until successful or kill this replica group (
	 *         {@code request.getServiceName()}) altogether after a limited
	 *         number of retries, so the replica group may get stuck unless it
	 *         returns true after a limited number of retries. Thus, with paxos
	 *         as the replica coordination protocol, returning false is not
	 *         really an option as paxos has no way to "roll back" a request
	 *         whose global order has already been agreed upon.
	 *         <p>
	 *         With replica coordination protocols other than paxos, the boolean
	 *         return value could be used in protocol-specific ways, e.g., a
	 *         primary-backup protocol might normally execute a request at just
	 *         one replica but relay the request to one or more backups if the
	 *         return value of {@link #execute(Request)} is false.
	 */
	@Override
	public boolean execute(Request request, boolean doNotReplyToClient) {
		// TODO: submit request to data store
		log.log(Level.INFO, "Replicable Giga Paxos called execute with request={0} and doNotReplyToClient={1}", new Object[]{request.toString(), doNotReplyToClient});

		throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		log.log(Level.INFO, "Replicable Giga Paxos called execute with request={0}", new Object[]{request.toString()});

		return true;
		// throw new RuntimeException("Not yet implemented");
	}

	/**
	 * Checkpoints the current application state and returns it.
	 * 
	 * @param name
	 * @return Returns the checkpoint state. If the application encounters an
	 *         error while creating the checkpoint, it must retry until
	 *         successful or throw a RuntimeException. Returning a null value
	 *         will be interpreted to mean that the application state is indeed
	 *         null.
	 *         <p>
	 *         Note that {@code state} may simply be an app-specific handle,
	 *         e.g., a file name, representing the state as opposed to the
	 *         actual state. The application is responsible for interpreting the
	 *         state returned by {@link #checkpoint(String)} and that supplied
	 *         in {@link #restore(String, String)} in a consistent manner.
	 */
	@Override
	public String checkpoint(String name) {
		// TODO:
		log.log(Level.INFO, "Replicable Giga Paxos called checkpoint with name={0}", new Object[]{name});

		// throw new RuntimeException("Not yet implemented");
		return "wassup";
	}


	/**
	 * Resets the current application state for {@code name} to {@code state}.
	 * <p>
	 * Note that {@code state} may simply be an app-specific handle, e.g., a
	 * file name, representing the state as opposed to the actual state. The
	 * application is responsible for interpreting the state returned by
	 * {@link #checkpoint(String)} and that supplied in
	 * {@link #restore(String, String)} in a consistent manner.
	 * 
	 * @param name
	 * @param state
	 * @return True if the app atomically updated the state successfully. Else,
	 *         it must throw an exception. If it returns false, the replica
	 *         coordination protocol may try to repeat the operation until
	 *         successful causing the application to get stuck.
	 */
	@Override
	public boolean restore(String name, String state) {
		// TODO:
		log.log(Level.INFO, "Replicable Giga Paxos called restore with name={0}, state={1} ", new Object[]{name, state});

		// throw new RuntimeException("Not yet implemented");
		return true;
	}


	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}
