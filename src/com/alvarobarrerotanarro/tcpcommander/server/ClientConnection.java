package com.alvarobarrerotanarro.tcpcommander.server;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

class ClientConnection {
	final Socket socket;
	final PrintWriter writer;
	final BufferedReader reader;
	
	private long lastPong;

	public ClientConnection(Socket socket, PrintWriter writer, BufferedReader reader) {
		this.socket = socket;
		this.writer = writer;
		this.reader = reader;
		this.lastPong = 0;
	}

	public void pong() {
		lastPong = System.currentTimeMillis();
	}
	
	public long getLastPong() {
		return lastPong;
	}

	@Override
	public String toString() {
		InetAddress addr = socket.getInetAddress();
		String ipv4 = addr.getHostAddress();
		int port = socket.getPort();

		return String.format("%s:%d", ipv4, port);
	}
}