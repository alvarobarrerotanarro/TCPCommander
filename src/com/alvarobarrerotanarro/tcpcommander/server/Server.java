package com.alvarobarrerotanarro.tcpcommander.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
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

	final private int port;
	final private AtomicBoolean running;

	final private ConcurrentHashMap<String, ClientConnection> clients;

	final private ConcurrentHashMap<String, String> tasks;
	final private Object tasksMonitor;

	final private Thread acceptThread;
	final private Thread dispatchThread;

	private final ConcurrentHashMap<EVENT, Consumer<Object>> eventHandlers;

	final private Logger logger;

	private class ClientConnection {
		final Socket socket;
		final PrintWriter writer;
		final BufferedReader reader;

		public ClientConnection(Socket socket, PrintWriter writer, BufferedReader reader) {
			this.socket = socket;
			this.writer = writer;
			this.reader = reader;
		}

		@Override
		public String toString() {
			InetAddress addr = socket.getInetAddress();
			String ipv4 = addr.getHostAddress();
			int port = socket.getPort();

			return String.format("%s:%d", ipv4, port);
		}
	}

	public Server(int port) {
		this.port = port;
		this.running = new AtomicBoolean(true);
		this.clients = new ConcurrentHashMap<>();
		this.tasks = new ConcurrentHashMap<>();
		this.tasksMonitor = new Object();

		acceptThread = new Thread(this::acceptLoop, "tcpcommander-accept-thread");
		dispatchThread = new Thread(this::dispatchLoop, "tcpcommander-ping-thread");

		eventHandlers = new ConcurrentHashMap<>();

		logger = Logger.getLogger(Server.class.getName());

		acceptThread.start();
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
			dispatchThread.join();
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}
	}

	private void addClient(Socket clientSocket) throws IOException {
		PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream(), true);
		BufferedReader clientReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

		ClientConnection client = new ClientConnection(clientSocket, clientWriter, clientReader);
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

	public void dispatchTask(String connectionName, String taskHeader, String taskBody) {
		if (clients.get(connectionName) == null) {
			return;
		}

		synchronized (tasksMonitor) {
			try {
				while (tasks.containsKey(connectionName)) {
					tasksMonitor.wait();
				}

				tasks.put(connectionName, taskHeader + ":" + taskBody);
				tasksMonitor.notifyAll();
			} catch (InterruptedException e) {

			}
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

	public void ping() {
		for (var it = clients.entrySet().iterator(); it.hasNext();) {
			var pair = it.next();
			String connectionName = pair.getKey();
			ClientConnection clientConnection = pair.getValue();

			clientConnection.writer.println("ping");
			logger.info("PING");

			if (clientConnection.writer.checkError()) {

				removeClient(connectionName);
				it.remove();

			} else {

				try {
					String line = clientConnection.reader.readLine();

					if (line == null || !line.equals("pong")) {
						removeClient(connectionName);
						it.remove();
					}

				} catch (IOException e) {
					logger.info(e.getMessage());
					removeClient(connectionName);
					it.remove();
				}

			}
		}
	}

	private void dispatchTasks() {
		for (var it = tasks.entrySet().iterator(); it.hasNext();) {
			var pair = it.next();
			String connectionName = pair.getKey();
			String task = pair.getValue();

			ClientConnection clientConnection = clients.get(connectionName);
			if (clientConnection != null) {
				clientConnection.writer.println(task);
				logger.info(String.format("TASK DISPATCH '%s'", task));
			}

			it.remove();

			synchronized (tasksMonitor) {
				tasksMonitor.notify();
			}
		}
	}

	private void dispatchLoop() {

		try {

			while (running.get()) {
				ping();
				dispatchTasks();
				Thread.sleep(PING_INTERVAL_MILLIS);
			}

		} catch (InterruptedException e2) {
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
