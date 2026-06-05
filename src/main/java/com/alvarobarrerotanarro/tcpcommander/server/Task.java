package com.alvarobarrerotanarro.tcpcommander.server;

/**
 * Representa una unidad inmutable de trabajo o comando que se transmite a
 * través de la red hacia los clientes TCP conectados.
 */
public class Task {
	/**
	 * Identificador principal de la tarea o tipo de comando.
	 */
	public final String head;

	/**
	 * Carga útil, parámetros o argumentos de la tarea.
	 */
	public final String body;

	/**
	 * Construye una nueva tarea especificando explícitamente la cabecera y el
	 * cuerpo.
	 *
	 * @param head Identificador del comando.
	 * @param body Carga útil de datos.
	 */
	public Task(String head, String body) {
		this.head = head;
		this.body = body;
	}

	/**
	 * Constructor de conveniencia para tareas simples que no requieren parámetros.
	 * <p>
	 * Delega internamente en {@link #Task(String, String)} utilizando un espacio en
	 * blanco ({@code " "}) como cuerpo por defecto.
	 * </p>
	 *
	 * @param head Identificador del comando.
	 */
	public Task(String head) {
		this(head, " ");
	}

	/**
	 * Reconstruye un objeto {@code Task} a partir de su representación textual
	 * estructurada proveniente de la red.
	 *
	 * @param taskStr Cadena de texto plana recibida del socket.
	 * @return Una nueva instancia de {@code Task} con los datos parseados.
	 */
	public static Task parse(String taskStr) {
		String headAndBody[] = taskStr.substring("TASK - ".length()).split(" : ");
		Task task = new Task(headAndBody[0], headAndBody[1]);
		return task;
	}

	/**
	 * Convierte la tarea en su formato de serialización estándar para transmisión.
	 *
	 * @return Cadena formateada bajo el patrón {@code "TASK - <head> : <body>"}.
	 */
	@Override
	public String toString() {
		return String.format("TASK - %s : %s", head, body);
	}
}