import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class AppServer {
  public static final int KEY_NOT_PRESENT = -1;

  // Store all the <Key, Value> pairs
  private ConcurrentSkipListMap<Integer, Integer> store;
  /**
   * Store the mapping of <KEY_HASH, LIST<KEY having hash as <KEY_HASH>>> Used
   * when we have to re-distribute.
   * 
   * The actual query will be --> all the keys whose hash is between <L, R>
   */
  private ConcurrentSkipListMap<Integer, ArrayList<Integer>> keyHashStore;
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
    keyHashStore = new ConcurrentSkipListMap<>();

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
    System.out.println("PUT <" + key + " " + value + ">");

    if (store.containsKey(key)) {
      store.replace(key, value);
      return;
    }
    // store the key with value
    store.put(key, value);
    // store the key with it's hash
    if (!keyHashStore.containsKey(LoadBalancer.keyHash(key))) {
      keyHashStore.put(LoadBalancer.keyHash(key), new ArrayList<>());
    }
    keyHashStore.get(LoadBalancer.keyHash(key)).add(key);
  }

  // Returns the value that corresponds to given 'key'
  public int get(int key) {
    System.out.println("GET <" + key + ">");

    if (store.containsKey(key)) {
      return store.get(key);
    }
    return KEY_NOT_PRESENT;
  }

  // get a map of <key, value> pair in the range (startKey, endKey)
  public ConcurrentNavigableMap<Integer, Integer> getKeyValueInRange(int startKey, int endKey) {
    System.out.println("getKeyValueInRange <" + startKey + " " + endKey + ">");

    // Prepare a map from the keyHashStore
    ConcurrentNavigableMap<Integer, ArrayList<Integer>> filteredKeyHashStore = keyHashStore.subMap(startKey, endKey);

    ConcurrentNavigableMap<Integer, Integer> result = new ConcurrentSkipListMap<>();
    for (Integer keyHash : filteredKeyHashStore.keySet()) {
      for (Integer key : filteredKeyHashStore.get(keyHash)) {
        result.put(key, store.get(key));
      }
    }

    return result;
  }

  // store a map of <key, value> pairs
  public void putKeyValues(ConcurrentNavigableMap<Integer, Integer> additionalStore) {
    // ****************************DEBUGGING PART******************************
    System.out.print("putKeyValues: <");
    NavigableSet<Integer> keys = additionalStore.keySet();
    for (Integer key : keys) {
      System.out.print("<" + key + " " + additionalStore.get(key) + "> ");
    }
    System.out.println(">");
    // ****************************DEBUGGING PART******************************

    for (Integer key : additionalStore.keySet()) {
      // To correctly insert into the "KEY_HASH_STORE" also
      this.put(key, additionalStore.get(key));
    }
  }

  // remove all the <keys> in the given set from our STORE
  public void removeKeyValues(NavigableSet<Integer> oldKeys) {
    // ****************************DEBUGGING PART******************************
    System.out.print("removeKeyValues: <");
    for (Integer key : oldKeys) {
      System.out.print("<" + key + "> ");
    }
    System.out.println(">");
    // ****************************DEBUGGING PART******************************

    for (Integer key : oldKeys) {
      store.remove(key);
      keyHashStore.get(LoadBalancer.keyHash(key)).remove(key);
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
        System.out.println("Listening on port " + this.LISTENING_PORT + " ....");
        // accept an incoming connection
        Socket clientRequest = server.accept();
        try {
          DataInputStream dis = new DataInputStream(clientRequest.getInputStream());

          // Client-Message must be in form "<DEST_IP> <DEST_PORT> <ACTUAL_MESSAGE>"
          String clientMessage = (String) dis.readUTF().trim();
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
          e.printStackTrace();
        } catch (NumberFormatException e) {
          // Malformed message
          // TODO: send response if required in future
          e.printStackTrace();
        }
      }

    } catch (Exception e) {
      // TODO: Modify this at last if needed
      e.printStackTrace();
    }
  }

  public static void main(String args[]) {
    if (args.length != 5) {
      System.out.println("Invalid arguments. Please provide arguments in the following format -->");
      System.out.println("[<L.B_IP> <L.B_port> <SERVER_ID> <MY_IP> <MY_PORT_TO_LISTEN_FOR_REQUEST>]");
      return;
    }
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