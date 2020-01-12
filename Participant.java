import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

enum ParticipantStatus {
	ONLINE, OFFLINE, NOT_MEMBER
}

public class Participant {

	// Attributes from the participant configuration.txt
	int id;
	String msgLogFileName;
	InetAddress coordinatorIP;
	int coordinatorPort;
	ParticipantStatus status = ParticipantStatus.NOT_MEMBER;

	// Participant details during msend as it receives incoming messages on a
	// different port
	int msendPort;
	InetAddress participantIP;
	ServerSocket msendServerSocket;
	Socket msendSocket;
	DataInputStream msendDis;

	boolean connected;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getMsgLogFileName() {
		return msgLogFileName;
	}

	public void setMsgLogFileName(String msgLogFileName) {
		this.msgLogFileName = msgLogFileName;
	}

	public InetAddress getCoordinatorIP() {
		return coordinatorIP;
	}

	public void setCoordinatorIP(InetAddress coordinatorIP) {
		this.coordinatorIP = coordinatorIP;
	}

	public int getCoordinatorPort() {
		return coordinatorPort;
	}

	public void setCoordinatorPort(int coordinatorPort) {
		this.coordinatorPort = coordinatorPort;
	}

	public ParticipantStatus getStatus() {
		return status;
	}

	public void setStatus(ParticipantStatus status) {
		this.status = status;
	}

	public int getMSendPort() {
		return msendPort;
	}

	public void setMSendPort(int msendPort) {
		this.msendPort = msendPort;
	}

	public InetAddress getParticipantIP() {
		return participantIP;
	}

	public void setParticipantIP(InetAddress participantIP) {
		this.participantIP = participantIP;
	}

	public ServerSocket getMSendServerSocket() {
		return msendServerSocket;
	}

	public DataInputStream getMSendDis() {
		return msendDis;
	}

	public boolean isConnected() {
		return connected;
	}

	public void setConnected(boolean connected) {
		this.connected = connected;
	}

	private void createMsgLogFile() {
		File f = new File(msgLogFileName);
		try {
			if (!f.exists()) {
				if (f.createNewFile()) {
					System.out.println("Created Message log file successfully");
				} else {
					System.out.println("Failed to create Message Log file");
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public void createMSendServerSocket() {
		try {
			msendServerSocket = new ServerSocket(msendPort);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public void createMSendConn() {
		try {
			msendSocket = msendServerSocket.accept();
			msendDis = new DataInputStream(msendSocket.getInputStream());
			setParticipantIP(msendSocket.getInetAddress());
			setConnected(true);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public void closeMSendConn() {
		try {
			msendDis.close();
			msendSocket.close();
			msendServerSocket.close();
			setConnected(false);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			List<String> configList = new ArrayList<String>();
			String pConfigFile = args[0];

			// Read the input file
			Scanner scanner = new Scanner(new File(pConfigFile));
			while (scanner.hasNextLine()) {
				configList.add(scanner.nextLine());
			}

			// Create participant object
			Participant p1 = new Participant();

			// Set the attributes
			p1.setId(Integer.parseInt(configList.get(0)));
			p1.setMsgLogFileName(configList.get(1));
			String[] coorConfig = configList.get(2).split(" ");
			p1.setCoordinatorIP(InetAddress.getByName(coorConfig[0]));
			p1.setCoordinatorPort(Integer.parseInt(coorConfig[1]));

			// Check if msg log file is present or not
			p1.createMsgLogFile();

			// Start the thread to accept user command
			ThreadA threadA = new ThreadA(p1);
			threadA.start();
		} catch (FileNotFoundException fne) {
			fne.printStackTrace();
		} catch (UnknownHostException uhe) {
			uhe.printStackTrace();
		}
	}
}

class ThreadA extends Thread {

	Socket socket;
	DataInputStream dis;
	DataOutputStream dos;

	String inputCmd;
	String output;

	Participant participant;

	public static final String PARTICIPANT_CONN_ACCEPT = "Coordinator accepted connection with Participant";
	public static final String PARTICIPANT_REGISTER_START = "Participant Register start";
	public static final String PARTICIPANT_REGISTER_NOTIFICATION = "Participant is added to the multicast group";
	public static final String SOCKET_CONN_CLOSED = "Closed socket connection";
	public static final String REGISTER_CMD = "register";
	public static final String DEREGISTER_CMD = "deregister";
	public static final String DISCONNECT_CMD = "disconnect";
	public static final String RECONNECT_CMD = "reconnect";
	public static final String MSEND_CMD = "msend";

	ThreadA(Participant participant) {
		this.participant = participant;
	}

	public void run() {
		try {
			socket = new Socket(participant.getCoordinatorIP(), participant.getCoordinatorPort());
			dis = new DataInputStream(socket.getInputStream());
			dos = new DataOutputStream(socket.getOutputStream());
			System.out.println("Connected to coordinator: " + socket);

			Scanner scanner = new Scanner(System.in);

			output = dis.readUTF();
			if (output.equals(PARTICIPANT_CONN_ACCEPT)) {
				while (true) {
					inputCmd = scanner.nextLine();
					String[] command = inputCmd.split(" ");
					switch (command[0]) {
					case REGISTER_CMD:
						register(command);
						break;
					case DEREGISTER_CMD:
						deregister();
						break;
					case DISCONNECT_CMD:
						disconnect();
						break;
					case RECONNECT_CMD:
						reconnect(command);
						break;
					case "msend":
						dos.writeUTF(inputCmd);
						output = dis.readUTF();
						System.out.println(output);
						break;
					}
				}
			} else {
				dis.close();
				dos.close();
				socket.close();
				System.out.println(output);
			}
		} catch (SocketException se) {
			System.out.println("Coordinator Socket closed");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void register(String[] command) {
		try {
			if(!participant.isConnected()) {
				// Create serversocket at participant side
				int msendPort = Integer.parseInt(command[1]);
				participant.setStatus(ParticipantStatus.ONLINE);
				participant.setMSendPort(msendPort);
				participant.createMSendServerSocket();
				participant.setParticipantIP(participant.getMSendServerSocket().getInetAddress());
	
				/*
				 * Starting ThreadB as it has to be operational before the participant sends the
				 * message to the coordinator
				 */
				ThreadB b = new ThreadB(participant);
				b.start();
	
				// Register participant id, IP and port number with coordinator
				String registerInput = REGISTER_CMD + " " + participant.getId() + ","
						+ participant.getParticipantIP().getHostAddress() + "," + msendPort;
				dos.writeUTF(registerInput);
				output = dis.readUTF();
				if (output.equals(PARTICIPANT_REGISTER_NOTIFICATION)) {
					participant.createMSendConn();
					System.out.println(output);
				} else {
					System.out.println(output);
//					try {
//						
////						dis.close();
////						dos.close();
////						socket.close();
//						
//					} catch (SocketException e) {
//						System.out.println("Socket closed");
//					} catch (IOException e) {
//						System.out.println(e.getMessage());
//					}
				}
			}else {
				System.out.println("Participant is already registered");
			}
		} catch (SocketException se) {
			System.out.println("Coordinator socket closed");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void deregister() {
		try {
			participant.setStatus(ParticipantStatus.NOT_MEMBER);
			participant.closeMSendConn();
			dos.writeUTF(inputCmd);
			output = dis.readUTF();
			System.out.println(output);
		} catch (SocketException se) {
			System.out.println("Coordinator socket closed");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void disconnect() {
		try {
			participant.setStatus(ParticipantStatus.OFFLINE);
			participant.closeMSendConn();
			dos.writeUTF(inputCmd);
			output = dis.readUTF();
			System.out.println(output);
		} catch (SocketException se) {
			System.out.println("Coordinator socket closed");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	private void reconnect(String[] command) {
		try {
			// Create serversocket at participant side
			int msendPort = Integer.parseInt(command[1]);
			participant.setStatus(ParticipantStatus.ONLINE);
			participant.setMSendPort(msendPort);
			participant.createMSendServerSocket();
			participant.setParticipantIP(participant.getMSendServerSocket().getInetAddress());

			/*
			 * Starting ThreadB as it has to be operational before the participant sends the
			 * message to the coordinator
			 */
			ThreadB b = new ThreadB(participant);
			b.start();

			// Register participant IP and port number with coordinator
			String reconnectInput = command[0] + " " + participant.getParticipantIP().getHostAddress() + ","
					+ msendPort;
			dos.writeUTF(reconnectInput);
			participant.createMSendConn();
			output = dis.readUTF();
			System.out.println(output);
		} catch (SocketException se) {
			System.out.println("Coordinator socket closed");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}

class ThreadB extends Thread {

	Participant participant;

	ThreadB(Participant participant) {
		this.participant = participant;
	}

	public void run() {
		try {
			boolean isLive;
			while (true) {
				if (Thread.currentThread().isAlive()) {
					isLive=true;
				}
				if (participant.getStatus() == ParticipantStatus.ONLINE && participant.isConnected()) {
					String msg = participant.getMSendDis().readUTF();
					BufferedWriter writer = new BufferedWriter(new FileWriter(participant.getMsgLogFileName(), true));
					writer.write(msg);
					writer.newLine();
					writer.close();
				}
				if (participant.getStatus() == ParticipantStatus.OFFLINE) {
					return;
				}
				if (participant.getStatus() == ParticipantStatus.NOT_MEMBER) {
					return;
				}
			}
		} catch (SocketException se) {
			System.out.println("Multicast socket closed");
		} catch (FileNotFoundException fnfe) {
			System.out.println(fnfe.getMessage());
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}