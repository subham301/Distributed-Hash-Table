import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class AppServer {
  public static final int KEY_NOT_PRESENT = -1;

  // Store all the <Key, Value> pairs
  private ConcurrentSkipListMap<Integer, Integer> store;
  // The address of the Load-Balancer
  private String loadBalancerIP;
  private int loadBalancerPort;
  // An ID for this server
  private final int SERVER_ID;
  // port number on which the current server will listen to for incoming request
  private final String MY_IP;
  private final int LISTENING_PORT;

  public AppServer(String loadBalancerIP, int loadBalancerPort, int serverID, String ipAdress, int listeningPort) {
    store = new ConcurrentSkipListMap<>();

    this.loadBalancerIP = loadBalancerIP;
    this.loadBalancerPort = loadBalancerPort;

    SERVER_ID = serverID;
    MY_IP = ipAdress;
    LISTENING_PORT = listeningPort;
  }

  // Returns the server-ID of the current server
  public int getServerId() {
    return SERVER_ID;
  }

  // store the given <key, value> pair
  public void put(int key, int value) {
    if (store.containsKey(key)) {
      store.replace(key, value);
      return;
    }
    store.put(key, value);
  }

  // Returns the value that corresponds to given 'key'
  public int get(int key) {
    if (store.containsKey(key)) {
      return store.get(key);
    }
    return KEY_NOT_PRESENT;
  }

  // get a map of <key, value> pair in the range (startKey, endKey)
  public ConcurrentNavigableMap<Integer, Integer> getKeyValueInRange(int startKey, int endKey) {
    return store.subMap(startKey, endKey);
  }

  // store a map of <key, value> pairs
  public void putKeyValues(ConcurrentNavigableMap<Integer, Integer> additionalStore) {
    store.putAll(additionalStore);
  }

  // remove all the <keys> in the given set from our STORE
  public void removeKeyValues(NavigableSet<Integer> oldKeys) {
    for (Integer key : oldKeys) {
      store.remove(key);
    }
  }

  // initiate the process of listening to a port
  private void initiate() {
    try {
      // Notify the load-balancer about the addition of this server
      Socket socket = new Socket(this.loadBalancerIP, this.loadBalancerPort);

      // SERVER <MY_IP> <MY_PORT> <OPERATION> <SERVER_ID>
      String message = "SERVER " + this.MY_IP + " " + this.LISTENING_PORT + " " + "ADD" + " " + this.SERVER_ID;
      DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
      outputStream.writeUTF(message);
      outputStream.flush();
      outputStream.close();

      socket.close();

      // Listens to a port for an incoming connection request
      ServerSocket server = new ServerSocket(this.LISTENING_PORT);
      while (true) {
        // accept an incoming connection
        Socket clientRequest = server.accept();
        try {
          DataInputStream dis = new DataInputStream(clientRequest.getInputStream());

          // Client-Message must be in form "<DEST_IP> <DEST_PORT> <ACTUAL_MESSAGE>"
          String clientMessage = (String) dis.readUTF();
          // Try to split the different part of message and if any of it is unavailable
          // then it means that message is not well structured and ignore it
          // Message: <DEST_IP> <DEST_PORT> <TASK => <OPERATION PARAMETERS> >
          String[] parameters = clientMessage.split(" ", 3);
          if (parameters.length < 3)
            throw new IndexOutOfBoundsException();

          // create a new task and let it handle the request and we will continue to
          // listen for the new reqeusts
          new AppServerTaskHandler(parameters[2], parameters[0], Integer.valueOf(parameters[1]), this).start();

          dis.close();

        } catch (IndexOutOfBoundsException e) {
          // Malformed message
          // TODO: send response if required in future
        } catch (NumberFormatException e) {
          // Malformed message
          // TODO: send response if required in future
        }
      }

    } catch (Exception e) {
      // TODO: Modify this at last if needed
      e.printStackTrace();
    }
  }

  public static void main(String args[]) {
    // TODO: make an AppServer object and properly initialize all values
    // Arguments passed:
    // <L.B_IP> <L.B_port> <SERVER_ID> <MY_IP> <MY_PORT_TO_LISTEN_FOR_REQUEST>
    try {
      if (args.length != 5)
        throw new NumberFormatException();

      AppServer myServer = new AppServer(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), args[3],
          Integer.valueOf(args[4]));

      myServer.initiate();

    } catch (NumberFormatException e) {
      // Malformed message
      // TODO: send response if required in future
    }
  }
}