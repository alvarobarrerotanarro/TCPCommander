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

public class Server implements AutoCloseable {
	public static enum EVENT {
		NEW_CLIENT, FORGOTTEN_CLIENT
	}

	private static final int ACCEPT_INTERVAL_MILLIS = 1000;
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

	public int getPort() {
		return port;
	}

	public void setLoggingLevel(Level lvl) {
		logger.setLevel(lvl);
	}

	public boolean isRunning() {
		return running.get();
	}

	public void setRunning(boolean running) {
		this.running.set(running);
	}

	public void addEventHandler(EVENT e, Consumer<Object> handler) {
		eventHandlers.put(e, handler);
	}

	public void removeEventHandlers(EVENT e) {
		eventHandlers.remove(e);
	}

	@Override
	public void close() {
		running.set(false);
	}

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
	 * Blocks the current thread until the task is registered. Therefore two
	 * consecutive calls to this method from the same thread will deadlock until the
	 * first task was sent to the client.
	 * 
	 * @param connectionName
	 * @param task
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

			Thread.sleep(ACCEPT_INTERVAL_MILLIS);

		} catch (IOException e2) {
			logger.severe(String.format("Server socket open failed: %s\n", e2.getMessage()));
		} catch (InterruptedException e2) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
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

	public String[] availableClients() {
		String clientConnections[] = new String[clients.size()];
		int clientConnectoinsPos = 0;

		for (Map.Entry<String, ClientConnection> pair : clients.entrySet()) {
			clientConnections[clientConnectoinsPos++] = pair.getKey();
		}

		return clientConnections;
	}
}
