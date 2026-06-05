package com.alvarobarrerotanarro.tcpcommander.server;

public class Task {
	public final String head;
	public final String body;

	public Task(String head, String body) {
		this.head = head;
		this.body = body;
	}

	public Task(String head) {
		this(head, " ");
	}

	public static Task parse(String taskStr) {
		String headAndBody[] = taskStr.substring("TASK - ".length()).split(" : ");
		Task task = new Task(headAndBody[0], headAndBody[1]);
		return task;
	}

	@Override
	public String toString() {
		return String.format("TASK - %s : %s", head, body);
	}
}
