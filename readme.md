
# TCP Commander – Documentación de uso

Este documento describe el uso del sistema cliente-servidor TCP Commander, orientado a ejecución de tareas remotas entre un servidor y múltiples clientes.

---

# 1. Concepto general

El sistema se basa en un modelo simple:

- El servidor mantiene conexiones TCP con múltiples clientes
- El servidor envía tareas (Task) a los clientes
- Los clientes ejecutan las tareas mediante handlers registrados
- Se mantiene la conexión activa mediante ping/pong automático

---

# 2. Uso del servidor

## 2.1 Inicialización

```java
import java.util.logging.Level;

public class MainServer {
    public static void main(String[] args) {
        Server server = new Server(5000);

        server.setLoggingLevel(Level.INFO);

        server.waitForClose();
    }
}
```

---

## 2.2 Eventos del servidor

El servidor permite reaccionar a conexiones y desconexiones de clientes.

```java
server.addEventHandler(Server.EVENT.NEW_CLIENT, data -> {
    System.out.println("Nuevo cliente conectado: " + data);
});

server.addEventHandler(Server.EVENT.FORGOTTEN_CLIENT, data -> {
    System.out.println("Cliente desconectado: " + data);
});
```

---

## 2.3 Enviar tareas a clientes

Cada cliente está identificado internamente por un `connectionName`.

```java
server.addTask("CLIENT_ID", new Task("PRINT", "Hola cliente"));
```

### Significado:

- "CLIENT_ID" → identificador del cliente
- "PRINT" → tipo de tarea
- "Hola cliente" → payload de la tarea

---

# 3. Uso del cliente

## 3.1 Inicialización

```java
import java.util.logging.Level;

public class MainClient {
    public static void main(String[] args) {

        Client client = new Client("127.0.0.1", 5000);

        client.setLoggingLevel(Level.INFO);

        client.waitForClose();
    }
}
```

---

## 3.2 Registro de handlers de tareas

El cliente ejecuta lógica en función del tipo de tarea (`head`).

```java
client.addTaskHandler("PRINT", body -> {
    System.out.println("MENSAJE DEL SERVIDOR: " + body);
});
```

---

## 3.3 Ejemplo con múltiples handlers

```java
client.addTaskHandler("PRINT", body -> {
    System.out.println("PRINT: " + body);
});

client.addTaskHandler("LOG", body -> {
    System.out.println("LOG: " + body);
});
```

---

# 4. Ejemplo completo

## 4.1 Servidor completo

```java
import java.util.logging.Level;

public class MainServer {
    public static void main(String[] args) {

        Server server = new Server(5000);
        server.setLoggingLevel(Level.INFO);

        server.addEventHandler(Server.EVENT.NEW_CLIENT, id -> {
            System.out.println("Conectado: " + id);

            server.addTask(
                (String) id,
                new Task("PRINT", "Bienvenido al sistema")
            );
        });

        server.waitForClose();
    }
}
```

---

## 4.2 Cliente completo

```java
import java.util.logging.Level;

public class MainClient {
    public static void main(String[] args) {

        Client client = new Client("127.0.0.1", 5000);
        client.setLoggingLevel(Level.INFO);

        client.addTaskHandler("PRINT", body -> {
            System.out.println("SERVER: " + body);
        });

        client.waitForClose();
    }
}
```

---

# 5. Flujo de ejecución

```
1. El cliente se inicia
2. Se conecta al servidor TCP
3. El servidor detecta la conexión
4. El servidor envía una Task
5. El cliente la recibe
6. El cliente ejecuta el handler correspondiente
7. Se mantiene conexión con ping/pong automático
```

---

# 6. Modelo mental

Servidor = emisor de tareas  
Cliente = ejecutor de tareas  

Flujo:

Server → Task → TCP → Client → Handler → Acción

---

# 7. Limitaciones del sistema

- No hay autenticación
- No hay cifrado (texto plano)
- Identificador de cliente no persistente
- Un solo handler por evento
- No hay prioridades de tareas
- Sin garantías avanzadas de entrega

---

# 8. Resumen

TCP Commander es un sistema ligero de:

- Comunicación TCP persistente
- Ejecución remota de tareas simples
- Arquitectura tipo server-worker
