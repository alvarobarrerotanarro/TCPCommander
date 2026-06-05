package com.alvarobarrerotanarro.tcpcommander.server;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Encapsula la información de conexión, flujos de E/S y estado del ciclo de
 * vida de un cliente TCP individual conectado al servidor.
 */
class ClientConnection {
	final Socket socket;
	final PrintWriter writer;
	final BufferedReader reader;

	/** Timestamp en milisegundos de la última respuesta 'pong' recibida. */
	private long lastPong;

	/**
	 * Construye una nueva abstracción de conexión para un cliente TCP activo.
	 *
	 * @param socket El socket de red devuelto por el servidor.
	 * @param writer El escritor de flujo asociado a la salida del socket.
	 * @param reader El lector de flujo asociado a la entrada del socket.
	 */
	public ClientConnection(Socket socket, PrintWriter writer, BufferedReader reader) {
		this.socket = socket;
		this.writer = writer;
		this.reader = reader;
		this.lastPong = 0;
	}

	/**
	 * Actualiza el registro temporal interno al momento actual del sistema.
	 * <p>
	 * Debe invocarse inmediatamente tras recibir con éxito una trama de
	 * confirmación de tipo "pong" por parte del cliente remoto.
	 * </p>
	 */
	public void pong() {
		lastPong = System.currentTimeMillis();
	}

	/**
	 * Obtiene el instante de tiempo en milisegundos en el que se registró la última
	 * confirmación de actividad del cliente.
	 */
	public long getLastPong() {
		return lastPong;
	}

	/**
	 * Genera un identificador único único en formato texto basado en la dirección
	 * IP y el puerto remoto del cliente.
	 *
	 * @return Cadena de caracteres formateada como {@code "IP:PUERTO"} (ej:
	 *         "192.168.1.50:52314").
	 */
	@Override
	public String toString() {
		InetAddress addr = socket.getInetAddress();
		String ipv4 = addr.getHostAddress();
		int port = socket.getPort();

		return String.format("%s:%d", ipv4, port);
	}
}