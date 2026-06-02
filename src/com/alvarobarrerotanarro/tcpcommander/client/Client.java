package com.alvarobarrerotanarro.tcpcommander.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.alvarobarrerotanarro.tcpcommander.server.Task;

public class Client implements AutoCloseable {
	public static enum EVENT {
		CONN_STATUS_CHANGE
	}

	private static final int READ_INTERVAL_MILLIS = 1000;
	private static final int RECONNECT_INTERVAL_MILLIS = 1000;

	final private String ipAddr;
	final private int port;
	private Socket socket;
	private PrintWriter writer;
	private BufferedReader reader;

	final private AtomicBoolean running;
	final private AtomicBoolean connected;

	final private Thread socketThread;
	final private Thread tasksThread;

	final private BlockingQueue<Task> tasks;
	final private Map<String, Consumer<String>> taskHandlers;

	final private ConcurrentHashMap<EVENT, Consumer<Object>> eventHandlers;

	final private Logger logger;

	public Client(String ipAddr, int port) {
		this.ipAddr = ipAddr;
		this.port = port;

		running = new AtomicBoolean(true);
		connected = new AtomicBoolean(false);

		tasks = new LinkedBlockingDeque<>();
		taskHandlers = new HashMap<>();

		eventHandlers = new ConcurrentHashMap<>();

		logger = Logger.getLogger(Client.class.getName());

		socketThread = new Thread(this::connectLoop);
		tasksThread = new Thread(this::tasksLoop);

		socketThread.start();
		tasksThread.start();
	}

	public void setLoggingLevel(Level lvl) {
		logger.setLevel(lvl);
	}

	public boolean isConnected() {
		return connected.get();
	}

	public boolean isRunning() {
		return running.get();
	}

	public void setRunning(boolean running) {
		this.running.set(running);
	}

	public void addTaskHandler(String taskName, Consumer<String> handler) {
		taskHandlers.put(taskName, handler);
	}

	public void removeTaskHandler(String taskName) {
		taskHandlers.remove(taskName);
	}

	public void addEventHandler(EVENT e, Consumer<Object> handler) {
		eventHandlers.put(e, handler);
	}

	public void removeEventHandlers(EVENT e) {
		eventHandlers.remove(e);
	}

	private void dispatchEvent(EVENT e, Object eventData) {
		Consumer<Object> handler = eventHandlers.get(e);
		if (handler != null) {
			handler.accept(eventData);
		}
	}

	private boolean pong() {
		writer.println("pong");
		return !writer.checkError();
	}

	public void connectLoop() {

		try {

			while (running.get()) {

				try {
					socket = new Socket(ipAddr, port);
					writer = new PrintWriter(socket.getOutputStream(), true);
					reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					dispatchEvent(EVENT.CONN_STATUS_CHANGE, true);
					logger.info("CONNECTED");

					connected.set(true);
					dispatchEvent(EVENT.CONN_STATUS_CHANGE, true);
					readLoop();
					connected.set(false);
					dispatchEvent(EVENT.CONN_STATUS_CHANGE, false);
				} catch (IOException e) {
					logger.warning(e.getMessage());
				}

				Thread.sleep(RECONNECT_INTERVAL_MILLIS);
			}

			try {
				socket.close();
			} catch (IOException e) {
				logger.info(e.getMessage());
			}

		} catch (InterruptedException e2) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}

	}

	private void readLoop() {

		try {

			while (running.get() && connected.get()) {

				try {
					String line = reader.readLine();

					if (line == null) {
						connected.set(false);
					} else {

						if (line.equals("ping")) {
							pong();
							logger.info("PONG");
						} else {

							try {
								Task task = Task.parse(line);
								tasks.add(task);
							} catch (Exception e) {
								logger.warning("TASK PARSING ERROR");
							}

						}

					}

				} catch (IOException e) {
					connected.set(false);
					logger.severe(e.getMessage());
				}

				Thread.sleep(READ_INTERVAL_MILLIS);
			}

		} catch (InterruptedException e2) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}

	}

	public void tasksLoop() {
		try {
			while (running.get()) {

				Task task = tasks.take();
				logger.info(String.format("'%s' RETRIEVAL", task.toString()));

				Consumer<String> handler = taskHandlers.get(task.head);
				if (handler != null) {
					handler.accept(task.body);
					logger.info(String.format("'%s' COMPLETED", task.toString()));
				}

			}
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}
	}

	public void waitForClose() {

		try {
			socketThread.join();
			tasksThread.join();
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}

	}

	@Override
	public void close() {
		running.set(false);
	}
}
