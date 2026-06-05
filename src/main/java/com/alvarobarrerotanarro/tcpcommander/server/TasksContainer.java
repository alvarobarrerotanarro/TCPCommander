package com.alvarobarrerotanarro.tcpcommander.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TasksContainer {
	private final Map<ClientConnection, Task> pending;
	private AtomicBoolean serverRunningState;

	public TasksContainer(AtomicBoolean serverRunningState) {
		pending = new HashMap<>();
		this.serverRunningState = serverRunningState;
	}

	/**
	 * Blocks the current thread until the previous map entry is removed. Therefore
	 * two consecutive calls to this method from the same thread will deadlock until
	 * some other thread calls completeTask method.
	 * 
	 * @param clientConnection
	 * @param task
	 * @throws InterruptedException
	 */
	public synchronized void addTask(ClientConnection clientConnection, Task task) throws InterruptedException {
		while (pending.containsKey(clientConnection) && serverRunningState.get()) {
			wait();
		}

		pending.put(clientConnection, task);
		notifyAll();
	}

	/**
	 * Marks the task associated with the client connection as completed and thus
	 * removes the map entry notifying the rest of thread.s
	 * 
	 * @param clientConnection
	 * @return true in case the removal of the map entry was satisfactory.
	 */
	public synchronized boolean completeTask(ClientConnection clientConnection) {
		boolean result = pending.remove(clientConnection) != null;
		notifyAll();
		return result;
	}

	/**
	 * Blocks the current thread until there is almost one pending tasks.
	 */
	public synchronized void waitForTask() throws InterruptedException {
		while (pending.size() == 0 && serverRunningState.get()) {
			wait();
		}
	}

	/**
	 * @return A copy of the internal tasks map.
	 */
	public Map<ClientConnection, Task> getPendingTasks() {
		return Map.copyOf(pending);
	}

	/**
	 * @return The number of pending tasks.
	 */
	public int size() {
		return pending.size();
	}
}
