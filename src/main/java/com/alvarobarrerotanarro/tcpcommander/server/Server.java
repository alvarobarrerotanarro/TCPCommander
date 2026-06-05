package com.alvarobarrerotanarro.tcpcommander.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TCP Server for managing persistent client connections and task dispatching.
 *
 * <p>
 * This server maintains multiple TCP clients, supports a heartbeat mechanism
 * (ping/pong), and allows sending asynchronous tasks to connected clients.
 * </p>
 *
 * <p>
 * Main features:
 * </p>
 * <ul>
 * <li>Multi-client TCP handling</li>
 * <li>Automatic client lifecycle management</li>
 * <li>Task dispatching system</li>
 * <li>Heartbeat (ping/pong) connection monitoring</li>
 * <li>Event system for client connection changes</li>
 * </ul>
 */
public class Server implements AutoCloseable {
	/**
	 * Events triggered by the server lifecycle.
	 */
	public static enum EVENT {
		NEW_CLIENT, FORGOTTEN_CLIENT
	}

	private static final int PING_INTERVAL_MILLIS = 1000;
	private static final int PONG_TIMEOUT_MILLIS = 3000;

	final private int port;
	final private AtomicBoolean running;

	final private ConcurrentHashMap<String, ClientConnection> clients;

	final private TasksContainer tasks;

	final private Thread acceptThread;
	final private Thread readThread;
	final private Thread heartbeatThread;
	final private Thread dispatchThread;

	private final ConcurrentHashMap<EVENT, Consumer<Object>> eventHandlers;

	final private Logger logger;

	/**
	 * Creates and starts a new TCP server.
	 *
	 * <p>
	 * Once instantiated, the server immediately starts internal threads:
	 * </p>
	 * <ul>
	 * <li>Connection accept loop</li>
	 * <li>Client read loop</li>
	 * <li>Heartbeat monitor loop</li>
	 * <li>Task dispatch loop</li>
	 * </ul>
	 *
	 * @param port TCP port where the server will listen for connections
	 */
	public Server(int port) {
		this.port = port;
		running = new AtomicBoolean(true);
		clients = new ConcurrentHashMap<>();
		tasks = new TasksContainer(running);

		acceptThread = new Thread(this::acceptLoop, "tcpcommander-accept-thread");
		readThread = new Thread(this::readLoop, "tcpcommander-read-thread");
		heartbeatThread = new Thread(this::heartbeatLoop, "tcpcommander-heartbeat-loop");
		dispatchThread = new Thread(this::dispatchLoop, "tcpcommander-ping-thread");

		eventHandlers = new ConcurrentHashMap<>();

		logger = Logger.getLogger(Server.class.getName());

		acceptThread.start();
		readThread.start();
		heartbeatThread.start();
		dispatchThread.start();
	}

	/**
	 * Returns the TCP port used by the server.
	 *
	 * @return port number
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Sets logging verbosity level.
	 *
	 * @param lvl desired logging level
	 */
	public void setLoggingLevel(Level lvl) {
		logger.setLevel(lvl);
	}

	/**
	 * Checks whether the server is currently running.
	 *
	 * @return true if running, false otherwise
	 */
	public boolean isRunning() {
		return running.get();
	}

	/**
	 * Enables or disables the server execution flag.
	 *
	 * <p>
	 * Note: This does NOT immediately stop threads, but signals them to terminate
	 * gracefully.
	 * </p>
	 *
	 * @param running new running state
	 */
	public void setRunning(boolean running) {
		this.running.set(running);
	}

	/**
	 * Registers an event handler for server events.
	 *
	 * @param e       event type
	 * @param handler callback executed when event occurs
	 */
	public void addEventHandler(EVENT e, Consumer<Object> handler) {
		eventHandlers.put(e, handler);
	}

	/**
	 * Removes the handler associated with the given event.
	 *
	 * @param e event type to remove
	 */
	public void removeEventHandlers(EVENT e) {
		eventHandlers.remove(e);
	}

	/**
	 * Gracefully stops the server execution.
	 *
	 * <p>
	 * This sets the internal running flag to false, allowing threads to terminate
	 * naturally.
	 * </p>
	 */

	@Override
	public void close() {
		running.set(false);
	}

	/**
	 * Blocks the current thread until all internal server threads finish execution.
	 *
	 * <p>
	 * This should be called after {@link #close()} to ensure clean shutdown.
	 * </p>
	 */
	public void waitForClose() {
		try {
			acceptThread.join();
			readThread.join();
			heartbeatThread.join();
			dispatchThread.join();
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}
	}

	private void addClient(Socket clientSocket) throws IOException {
		PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream(), true);
		BufferedReader clientReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

		ClientConnection client = new ClientConnection(clientSocket, clientWriter, clientReader);
		client.pong();
		clients.put(client.toString(), client);

		dispatchEvent(EVENT.NEW_CLIENT, client.toString());
	}

	private void removeClient(String connectionName) {
		ClientConnection clientConnection = clients.get(connectionName);
		clients.remove(connectionName);

		try {
			clientConnection.socket.close();
		} catch (IOException e) {
		}

		dispatchEvent(EVENT.FORGOTTEN_CLIENT, connectionName);
	}

	private void dispatchEvent(EVENT e, Object eventData) {
		Consumer<Object> handler = eventHandlers.get(e);
		if (handler != null) {
			handler.accept(eventData);
		}
	}

	/**
	 * Sends a task to a specific client.
	 *
	 * <p>
	 * This method blocks until the task is accepted by the internal queue. Calling
	 * it repeatedly from the same thread may cause deadlocks.
	 * </p>
	 *
	 * @param connectionName identifier of the target client
	 * @param task           task to execute remotely
	 */
	public void addTask(String connectionName, Task task) {
		ClientConnection clientConnection;

		if ((clientConnection = clients.get(connectionName)) == null) {
			return;
		}

		try {
			tasks.addTask(clientConnection, task);
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}
	}

	private void dispatchTasks() {
		Map<ClientConnection, Task> tasksMap = tasks.getPendingTasks();

		for (var it = tasksMap.entrySet().iterator(); it.hasNext();) {
			var pair = it.next();
			ClientConnection clientConnection = pair.getKey();
			Task task = pair.getValue();

			clientConnection.writer.println(task);
			logger.info(String.format("DISPATCH '%s'", task));

			tasks.completeTask(clientConnection);
		}
	}

	private void acceptLoop() {
		try (ServerSocket serverSocket = new ServerSocket(port)) {

			try {
				while (running.get()) {

					Socket clientSocket = serverSocket.accept();
					addClient(clientSocket);

				}
			} catch (IOException e) {
				logger.warning(e.getMessage());
			}

		} catch (IOException e2) {
			logger.severe(String.format("Server socket open failed: %s\n", e2.getMessage()));
		}
	}

	private void readLoop() {

		while (running.get()) {

			for (ClientConnection clientConnection : clients.values()) {

				try {
					String line = clientConnection.reader.readLine();

					if (line != null && line.equals("pong")) {
						clientConnection.pong();
					}

				} catch (IOException e) {
					logger.warning(e.getMessage());
				}

			}

		}

	}

	private void heartbeatLoop() {

		try {
			while (running.get()) {

				for (ClientConnection clientConnection : new ArrayList<>(clients.values())) {

					if (System.currentTimeMillis() - clientConnection.getLastPong() < PONG_TIMEOUT_MILLIS) {
						clientConnection.writer.println("ping");
						logger.info("PING to " + clientConnection);
					} else {
						removeClient(clientConnection.toString());
					}

				}

				Thread.sleep(PING_INTERVAL_MILLIS);
			}
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}

	}

	private void dispatchLoop() {

		try {
			while (running.get()) {

				tasks.waitForTask();
				dispatchTasks();

			}
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}
	}

	/**
	 * Returns a snapshot of all currently connected clients.
	 *
	 * @return array of client identifiers
	 */
	public String[] availableClients() {
		String clientConnections[] = new String[clients.size()];
		int clientConnectoinsPos = 0;

		for (Map.Entry<String, ClientConnection> pair : clients.entrySet()) {
			clientConnections[clientConnectoinsPos++] = pair.getKey();
		}

		return clientConnections;
	}
}
