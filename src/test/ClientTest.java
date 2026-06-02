package test;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.SwingConstants;
import javax.swing.WindowConstants;

import com.alvarobarrerotanarro.tcpcommander.client.Client;
import java.awt.Font;

public class ClientTest extends JFrame {

	private Client client;
	private JLabel lblConnectionStatus;

	public ClientTest() {
		super();
		setSize(800, 600);
		setTitle("TCP Client");
		setLocationRelativeTo(null);
		setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
		getContentPane().setLayout(new BorderLayout(0, 0));

		configClient();

		addWindowListener(new WindowAdapter() {
			public void windowClosed(WindowEvent e) {
				client.close();
			}
		});

		lblConnectionStatus = new JLabel("");
		lblConnectionStatus.setFont(new Font("Tahoma", Font.BOLD, 34));
		lblConnectionStatus.setHorizontalAlignment(SwingConstants.CENTER);
		lblConnectionStatus.setText("Disconnected");
		lblConnectionStatus.setForeground(Color.RED);
		getContentPane().add(lblConnectionStatus, BorderLayout.CENTER);

	}

	public void configClient() {
		String portStr = JOptionPane.showInputDialog("Enter server port");
		int port = 3000;

		String ipAddr = JOptionPane.showInputDialog("Enter server IP");

		ipAddr = ipAddr == null || ipAddr.length() == 0 ? "localhost" : ipAddr;
		try {
			port = Integer.parseInt(portStr);
		} catch (NumberFormatException e) {
		}

		// "130.110.235.176"
		client = new Client(ipAddr, port);

		client.addTaskHandler("msg", (body) -> {
			JOptionPane.showMessageDialog(null, body);
		});

		client.addTaskHandler("shutdown", (body) -> {
			JOptionPane.showMessageDialog(null, "Bye");
			client.close();
			System.exit(0);
		});

		client.addEventHandler(Client.EVENT.CONN_STATUS_CHANGE, (eventData) -> {
			boolean connStatus = (boolean) eventData;
			lblConnectionStatus.setText(connStatus ? "Connected" : "Disconnected");
			lblConnectionStatus.setForeground(connStatus ? Color.GREEN : Color.RED);
		});
	}

	public static void main(String args[]) {
		ClientTest app = new ClientTest();
		app.setVisible(true);

		// 130.110.235.176
	}
}
