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

/**
 * Cliente TCP persistente y reactivo con reconexión automática, monitorización
 * de latido (ping/pong) y despacho asíncrono de tareas. *
 * <p>
 * Esta clase implementa {@link AutoCloseable} para permitir un apagado ordenado
 * de sus hilos internos de red y procesamiento de tareas.
 * </p>
 */
public class Client implements AutoCloseable {

	/** Eventos del ciclo de vida del cliente notificables externamente. */
	public static enum EVENT {
		/**
		 * Notificado cuando cambia el estado de conexión del socket
		 * (conectado/desconectado).
		 */
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

	/**
	 * Cola con bloqueo que almacena las tareas parseadas pendientes de ejecución.
	 */
	final private BlockingQueue<Task> tasks;
	final private Map<String, Consumer<String>> taskHandlers;

	final private ConcurrentHashMap<EVENT, Consumer<Object>> eventHandlers;

	final private Logger logger;

	/**
	 * Crea e inicializa una nueva instancia del Cliente TCP.
	 * <p>
	 * Inicia de manera inmediata dos hilos de ejecución internos independientes:
	 * uno para la gestión de conexión/lectura y otro para el consumo de tareas.
	 * </p>
	 *
	 * @param ipAddr Dirección IP o Hostname del servidor remoto.
	 * @param port   Puerto TCP donde el servidor está escuchando.
	 */
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

	/**
	 * Configura el nivel de detalle de las trazas de registro (Logs).
	 *
	 * @param lvl Nivel de logging deseado.
	 */
	public void setLoggingLevel(Level lvl) {
		logger.setLevel(lvl);
	}

	/**
	 * Informa el estado actual de la conexión de red con el servidor.
	 *
	 * @return true si el cliente está conectado activamente; false en caso contrario.
	 */
	public boolean isConnected() {
		return connected.get();
	}

	/**
	 * Comprueba si el cliente sigue en ejecución general.
	 *
	 * @return true si los bucles internos están activos; false si se ha invocado {@link #close()}.
	 */
	public boolean isRunning() {
		return running.get();
	}

	/**
	 * Permite forzar o alterar el estado de ejecución general del cliente.
	 *
	 * @param running Nuevo estado operativo del cliente.
	 */
	public void setRunning(boolean running) {
		this.running.set(running);
	}

	/**
	 * Registra un manejador para procesar un tipo específico de comando de tarea.
	 *
	 * @param taskName El valor del campo 'head' de la {@link Task} que disparará el manejador.
	 * @param handler Callback que recibirá el campo 'body' de la tarea como argumento.
	 */
	public void addTaskHandler(String taskName, Consumer<String> handler) {
		taskHandlers.put(taskName, handler);
	}

	/**
	 * Elimina el manejador asociado a un comando de tarea concreto.
	 *
	 * @param taskName Nombre del identificador o cabecera a remover.
	 */
	public void removeTaskHandler(String taskName) {
		taskHandlers.remove(taskName);
	}

	/**
	 * Registra un callback para escuchar eventos del ciclo de vida del cliente.
	 *
	 * @param e Tipo de evento.
	 * @param handler Callback que recibirá la información del evento (generalmente un Booleano).
	 */
	public void addEventHandler(EVENT e, Consumer<Object> handler) {
		eventHandlers.put(e, handler);
	}

	/**
	 * Elimina todos los manejadores vinculados a un evento específico.
	 *
	 * @param e Tipo de evento a limpiar.
	 */
	public void removeEventHandlers(EVENT e) {
		eventHandlers.remove(e);
	}

	private void dispatchEvent(EVENT e, Object eventData) {
		Consumer<Object> handler = eventHandlers.get(e);
		if (handler != null) {
			handler.accept(eventData);
		}
	}

	/**
	 * Responde con una confirmación de latido ("pong") hacia el servidor.
	 * * @return true si el mensaje se envió con éxito; false si se detectó un error en el flujo de salida.
	 */
	private boolean pong() {
		writer.println("pong");
		return !writer.checkError();
	}

	/**
	 * Bucle principal de control del socket. Gestiona la instanciación de la conexión,
	 * la inicialización de flujos de E/S y el intento cíclico de reconexión tras fallos.
	 */
	private void connectLoop() {

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

	/**
	 * Bucle de lectura persistente sobre el flujo de entrada del socket.
	 * Interpreta los mensajes entrantes como señales "ping" o cadenas serializadas de tareas.
	 */
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

	/**
	 * Bucle de procesamiento de tareas. Consume de forma bloqueante la cola interna,
	 * localiza el manejador de negocio adecuado mediante el campo 'head' y lo ejecuta.
	 */
	private void tasksLoop() {
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

	/**
	 * Bloquea el hilo invocador hasta que los hilos internos de red y tareas 
	 * finalicen por completo su ejecución.
	 */
	public void waitForClose() {

		try {
			socketThread.join();
			tasksThread.join();
		} catch (InterruptedException e) {
			logger.info(String.format("Interrupted signal in '%s'", Thread.currentThread().getName()));
		}

	}

	/**
	 * Detiene de manera formal la ejecución del cliente enviando la señal de apagado 
	 * a los bucles concurrentes.
	 */
	@Override
	public void close() {
		running.set(false);
	}
}
