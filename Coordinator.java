import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

enum ParticipantStatus {
	ONLINE, OFFLINE, NOT_MEMBER
}

public class Coordinator implements Runnable {

	int portNo;
	static long threshold;

	int MAX_THREADS = 5;
	ThreadPoolExecutor threadpool = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREADS);

	static Map<Integer, MSendParticipant> participantMap = new HashMap<Integer, MSendParticipant>();
	
	public static final String COORDINATOR_WAIT_MSG = "Waiting for participants to connect";
	public static final String PARTICIPANT_UNABLE_ACCEPT = "Coordinator cannot accept a new participant currently.Please check later";
	public static final String PARTICIPANT_CONN_ACCEPT = "Coordinator accepted connection with Participant";

	public Coordinator(int portNo) {
		this.portNo = portNo;
	}

	/**
	 * In the run method,we wait for a participant to connect. Once a participant is
	 * connected, we check if there are threads available in thread pool to service
	 * the participant. If threads are available, then a thread attends to
	 * participant, else we terminate the participant connection
	 */
	@Override
	public void run() {
		ServerSocket serverSocket;
		Socket socket;
		DataInputStream dis;
		DataOutputStream dos;

		try {
			serverSocket = new ServerSocket(portNo);
			serverSocket.setSoTimeout(10000000);

			while (true) {
				System.out.println(COORDINATOR_WAIT_MSG);
				socket = serverSocket.accept();
				dis = new DataInputStream(socket.getInputStream());
				dos = new DataOutputStream(socket.getOutputStream());

				// Check if threads are available in thread pool
				if (threadpool.getPoolSize() == MAX_THREADS) {
					System.out.println(PARTICIPANT_UNABLE_ACCEPT);
					dos.writeUTF(PARTICIPANT_UNABLE_ACCEPT);
					dos.flush();
					dos.close();
					dis.close();
					socket.close();
					continue;
				}
				System.out.println("Connected to participant: " + socket);
				dos.writeUTF(PARTICIPANT_CONN_ACCEPT);
				dos.flush();
				threadpool.execute(new CoordinatorWorker(socket, dis, dos));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			threadpool.shutdown();
			try {
				threadpool.awaitTermination(2, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/*
	 * This class contains all information related to a participant while sending
	 * messages and receiving messages
	 */
	static class MSendParticipant {

		private int participantId;
		private InetAddress participantIP;
		private int msendPort;
		private ParticipantStatus status;
		private DataInputStream msendDis;
		private DataOutputStream msendDos;
		Socket msendSocket;
		LinkedList<MessageBean> messageList;

		MSendParticipant(int participantId, InetAddress participantIP, int msendPort, ParticipantStatus status) {
			this.participantId = participantId;
			this.participantIP = participantIP;
			this.msendPort = msendPort;
			this.status = status;
			messageList=new LinkedList<MessageBean>();
		}

		public int getParticipantID() {
			return participantId;
		}

		public void setParticipantID(int participantId) {
			this.participantId = participantId;
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

		public ParticipantStatus getStatus() {
			return status;
		}

		public void setStatus(ParticipantStatus status) {
			this.status = status;
		}

		public DataInputStream getMSendDis() {
			return msendDis;
		}

		public DataOutputStream getMSendDos() {
			return msendDos;
		}

		public void createMSendConn() {
			try {
				msendSocket = new Socket(participantIP, msendPort);
				msendDis = new DataInputStream(msendSocket.getInputStream());
				msendDos = new DataOutputStream(msendSocket.getOutputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void closeMSendConn() {
			try {
				msendDis.close();
				msendDos.close();
				msendSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		try {
			List<Integer> configList = new ArrayList<Integer>();
			String cConfigFile = args[0];

			// Read the input file
			Scanner scanner = new Scanner(new File(cConfigFile));
			while (scanner.hasNextLine()) {
				configList.add(Integer.parseInt(scanner.nextLine()));
			}

			// Create coordinator object
			Coordinator coordinator = new Coordinator(configList.get(0));
			Coordinator.threshold=configList.get(1);
			
			// Pass the runnable to thread
			Thread thread = new Thread(coordinator);
			thread.start();

		} catch (FileNotFoundException fne) {
			fne.printStackTrace();
		}
	}
}

/*
 * This class contains message information
 */
class MessageBean {

	private int participantId;
	private String message;
	private long timestamp;

	MessageBean(int participantId, String message) {
		this.participantId = participantId;
		this.message = message;
		this.timestamp = System.currentTimeMillis();
	}

	public int getParticipantId() {
		return participantId;
	}

	public void setParticipantId(int participantId) {
		this.participantId = participantId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}

/*
 * This class is responsible for servicing each participant. A new instance of
 * this class is created for each Participant.
 */
class CoordinatorWorker implements Runnable {

	Socket socket;
	DataInputStream dis;
	DataOutputStream dos;

	int participantId;
	InetAddress participantIP;
	int msendPort;
	
	public static final String PARTICIPANT_REGISTER_NOTIFICATION = "Participant is added to the multicast group";
	public static final String PARTICIPANT_REGISTER_ERROR = "Participant with same id already exists";
	public static final String PARTICIPANT_DEREGISTER_NOTIFICATION = "Participant is removed from multicast group";
	public static final String PARTICIPANT_DEREGISTER_ERROR = "Participant is not registered.Please check";
	public static final String PARTICIPANT_DISCONNECT_NOTIFICATION = "Participant is disconnected and will receive messages when it comes online";
	public static final String PARTICIPANT_DISCONNECT_ERROR = "Participant is not registered.Please check";
	public static final String PARTICIPANT_RECONNECT_NOTIFICATION = "Participant is reconnected and will be receiving messages sent within threshold";
	public static final String PARTICIPANT_MSEND_NOTIFICATION = "Multicast message is received by coordinator and will be send to all the participants";

	CoordinatorWorker(Socket socket, DataInputStream dis, DataOutputStream dos) {
		this.socket = socket;
		this.dis = dis;
		this.dos = dos;
	}

	@Override
	public void run() {
		String input;
		try {
			while (true) {
				input = dis.readUTF();
				String command[] = input.split(" ");
				switch (command[0]) {
				case "register":
					register(command);
					break;
				case "deregister":
					deregister();
					break;
				case "disconnect":
					disconnect();
					break;
				case "reconnect":
					reconnect(command);
					break;
				case "msend":
					msend(input);
					break;
				default:
					System.out.println("Invalid input");
					dos.writeUTF("Invalid input");
					dos.flush();
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void register(String[] command) {
		try {
			String participantInfo = command[1];

			/*
			 * Info is in the order of participantId, participantId IP and port number where
			 * its thread-B will receive multicast messages
			 */
			String[] info = participantInfo.split(",");
			participantId = Integer.parseInt(info[0]);
			participantIP = InetAddress.getByName(info[1]);
			msendPort = Integer.parseInt(info[2]);

			if(!Coordinator.participantMap.keySet().contains(participantId)) {
				dos.writeUTF(PARTICIPANT_REGISTER_NOTIFICATION);
				dos.flush();
				Coordinator.MSendParticipant p = new Coordinator.MSendParticipant(participantId, participantIP, msendPort,
						ParticipantStatus.ONLINE);
				p.createMSendConn();
				Coordinator.participantMap.put(participantId, p);
				System.out.println("Ready to send messages to participant :" + participantId);
			}else {
				dos.writeUTF(PARTICIPANT_REGISTER_ERROR);
				dos.flush();
				System.out.println("Participant with same id already exists");
			}	
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void deregister() {
		try {
			Coordinator.MSendParticipant p = Coordinator.participantMap.get(participantId);
			p.setStatus(ParticipantStatus.NOT_MEMBER);
			p.closeMSendConn();
			Coordinator.participantMap.remove(this.participantId);
			System.out.println(PARTICIPANT_DEREGISTER_NOTIFICATION);
			dos.writeUTF(PARTICIPANT_DEREGISTER_NOTIFICATION);
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void disconnect() {
		try {
			Coordinator.MSendParticipant p = Coordinator.participantMap.get(participantId);
			p.setStatus(ParticipantStatus.OFFLINE);
			p.closeMSendConn();
			System.out.println(PARTICIPANT_DISCONNECT_NOTIFICATION);
			dos.writeUTF(PARTICIPANT_DISCONNECT_NOTIFICATION);
			dos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void reconnect(String[] command) {
		try {
			String participantInfo = command[1];

			/*
			 * Info is in the order of participantId IP and port number where its thread-B
			 * will receive multicast messages
			 */
			String[] info = participantInfo.split(",");
			participantIP = InetAddress.getByName(info[0]);
			msendPort = Integer.parseInt(info[1]);

			Coordinator.MSendParticipant p = Coordinator.participantMap.get(participantId);
			p.setParticipantIP(participantIP);
			p.setMSendPort(msendPort);
			p.createMSendConn();
			p.setStatus(ParticipantStatus.ONLINE);
			dos.writeUTF(PARTICIPANT_RECONNECT_NOTIFICATION);
			dos.flush();
			
			while(!p.messageList.isEmpty()){
				MessageBean m = p.messageList.poll();
				if((System.currentTimeMillis() - m.getTimestamp())/1000 <= Coordinator.threshold){
					DataOutputStream msendDos = p.getMSendDos();
					msendDos.writeUTF(m.getMessage());
					msendDos.flush();
				}
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void msend(String cmdStr) {
		String message = cmdStr.substring(6);
		try {
			dos.writeUTF(PARTICIPANT_MSEND_NOTIFICATION);
			dos.flush();
			for (int pid : Coordinator.participantMap.keySet()) {
				Coordinator.MSendParticipant p = Coordinator.participantMap.get(pid);
				if (p.getStatus() == ParticipantStatus.ONLINE) {
					DataOutputStream msendDos = p.getMSendDos();
					msendDos.writeUTF(message);
					msendDos.flush();
				} else if (p.getStatus() == ParticipantStatus.OFFLINE) {
					MessageBean msgObj = new MessageBean(pid, message);
					p.messageList.add(msgObj);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}