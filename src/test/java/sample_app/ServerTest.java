package sample_app;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.AbstractListModel;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.WindowConstants;

import com.alvarobarrerotanarro.tcpcommander.server.Server;
import com.alvarobarrerotanarro.tcpcommander.server.Task;

public class ServerTest extends JFrame {

	private Server server;
	private JList connectedClientsList;
	private JList availableActionsList;
	private JTextArea txtMessage;
	private JButton btnAttack;

	public ServerTest() {
		super();

		configServer();

		setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
		setTitle("TCP Server");
		getContentPane().setBounds(0, 0, 800, 600);
		setSize(800, 600);
		setLocationRelativeTo(null);

		addWindowListener(new WindowAdapter() {
			public void windowClosed(WindowEvent e) {
				server.close();
				System.out.println("Server closed.");
			}
		});
		getContentPane().setLayout(new GridLayout(0, 1, 10, 10));

		JPanel titlePanel = new JPanel();
		getContentPane().add(titlePanel);
		titlePanel.setLayout(new BorderLayout(0, 0));

		JLabel lblNewLabel = new JLabel("Welcome to RED Team");
		lblNewLabel.setForeground(new Color(255, 0, 0));
		lblNewLabel.setFont(new Font("Tahoma", Font.BOLD, 28));
		lblNewLabel.setHorizontalAlignment(SwingConstants.CENTER);
		titlePanel.add(lblNewLabel);

		JPanel btnContainerPanel = new JPanel();
		getContentPane().add(btnContainerPanel);
		btnContainerPanel.setLayout(new GridLayout(0, 3, 10, 10));

		connectedClientsList = new JList();
		connectedClientsList.setToolTipText("Client Sessions");
		connectedClientsList.setFont(new Font("Tahoma", Font.PLAIN, 18));
		btnContainerPanel.add(connectedClientsList);

		availableActionsList = new JList();
		availableActionsList.setToolTipText("Hacking Tools");
		availableActionsList.setFont(new Font("Tahoma", Font.PLAIN, 18));
		btnContainerPanel.add(availableActionsList);
		availableActionsList.setModel(new AbstractListModel() {
			String[] values = new String[] { "Send message", "Shutdown client" };

			public int getSize() {
				return values.length;
			}

			public Object getElementAt(int index) {
				return values[index];
			}
		});

		txtMessage = new JTextArea();
		txtMessage.setToolTipText("Payload");
		txtMessage.setFont(new Font("Monospaced", Font.PLAIN, 18));
		btnContainerPanel.add(txtMessage);

		btnAttack = new JButton("Attack");
		btnAttack.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				int selectedAttack = availableActionsList.getSelectedIndex();

				switch (selectedAttack) {
				case 0 -> sendMessage();
				case 1 -> shutdown();
				}

			}
		});
		btnAttack.setFont(new Font("Tahoma", Font.PLAIN, 28));
		getContentPane().add(btnAttack);
	}

	public void configServer() {
		String portStr = JOptionPane.showInputDialog("Enter server port");
		int port = 3000;

		try {
			port = Integer.parseInt(portStr);
		} catch (NumberFormatException e) {
		}

		server = new Server(port);

		server.addEventHandler(Server.EVENT.NEW_CLIENT, this::refreshConnectionList);
		server.addEventHandler(Server.EVENT.FORGOTTEN_CLIENT, this::refreshConnectionList);

		System.out.println("Server at :" + port);
	}

	public void refreshConnectionList(Object eventData) {
		connectedClientsList.setModel(new AbstractListModel() {
			public int getSize() {
				return server.availableClients().length;
			}

			public Object getElementAt(int index) {
				return server.availableClients()[index];
			}
		});
	}

	public void sendMessage() {
		String connectionName = (String) connectedClientsList.getSelectedValue();
		String message = txtMessage.getText();
		server.addTask(connectionName, new Task("msg", message));
	}

	public void shutdown() {
		String connectionName = (String) connectedClientsList.getSelectedValue();
		server.addTask(connectionName, new Task("shutdown"));
	}

	public static void main(String args[]) {
		ServerTest app = new ServerTest();
		app.setVisible(true);
	}
}
