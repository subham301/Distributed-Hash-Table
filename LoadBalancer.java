import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

class LoadBalancer {

  private static final int MOD = 15;

  // ************ Static functions and data-members ***************
  /*
   * Our ith hash function is ==> (primes[i]^serverID) % MOD And for the 'key' we
   * use [(3 ^ key) % mod]
   */
  private static List<Integer> primes;
  private static final int HASHES = 3;

  // calculate a^b modulus MOD
  private static int fastPow(long a, long b) {
    if (a == 0)
      return 0;

    a %= MOD;
    long ans = 1;
    while (b > 0) {
      if ((b & 1) == 1)
        ans = (ans * a) % MOD;
      a = (a * a) % MOD;
      b /= 2;
    }
    return (int) ans;
  }

  private static boolean isPrime(int x) {
    for (int j = 2; j * j <= x; j++) {
      if (x % j == 0)
        return false;
    }
    return true;
  }

  static {
    primes = new ArrayList<>();
    int p = 1;
    while (primes.size() < HASHES) {
      if (isPrime(p))
        primes.add(p);
      p++;
    }
  }

  private static int getHash(int serverID, int byHash) {
    assert (byHash > 0 && byHash <= HASHES);
    return fastPow(primes.get(byHash - 1), serverID);
  }
  // ********* Static functions and data-members ENDS ***************

  // store the (position, serverID) in the consistent ring
  private ConcurrentSkipListSet<RingElement> consistentRing;
  // Store the list of position to which a particular server is mapped
  private Map<Integer, List<Integer>> serverPosition;
  // store the serverInfo for a particular SERVER_ID
  private Map<Integer, ServerInfo> servers;
  // Listening port and IP
  private final String IP;
  private final int PORT;

  public LoadBalancer(String ip, int port) {
    this.IP = ip;
    this.PORT = port;

    consistentRing = new ConcurrentSkipListSet<RingElement>();
    serverPosition = new ConcurrentHashMap<>();
    servers = new ConcurrentHashMap<>();
  }

  /**
   * Take the load in range [L, R] from the mentioned server and assign it to the
   * other server
   * 
   * returns true or false depending on whether the re-distribution happened
   * correctly or not
   */
  public boolean reDistributeLoad(ServerInfo from, ServerInfo to, int L, int R) {
    /**
     * **********************************************************************************
     * All the message to the server is of the form =>
     * 
     * <DESTINATION_IP> <DESTINATION_PORT> <TASK>
     * 
     * ==> <TASK> ==>
     * 
     * PUT_ALL <KEY1> <VALUE1> <KEY2> <VALUE2> <KEY3> <VALUE3> .... <KEYN> <VALUEN>
     * 
     * RE_DISTRIBUTE <FROM_KEY> <TO_KEY>
     * 
     * PUT <KEY> <VALUE>
     * 
     * GET <KEY>
     * 
     * *********************************************************************************
     */
    try {
      Socket socket = new Socket(from.getIP(), from.getPORT());
      DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());

      String task = "RE_DISTRIBUTE";
      task += " " + L + " " + R;

      outputStream.writeUTF(to.getIP() + " " + to.getPORT() + " " + task);
      outputStream.flush();
      outputStream.close();

      socket.close();

    } catch (Exception e) {
      return false;
    }
    return true;
  }

  // add the new server having id "serverID"
  public void addServer(ServerInfo serverInfo) {
    // Store the serverInfo with the loadBalancer itself
    servers.put(serverInfo.getID(), serverInfo);
    // List of all the positions after which and before the next server this server
    // is responsible for
    List<Integer> curServerPositions = new ArrayList<>();

    for (int j = 1; j <= HASHES; j++) {
      int position = getHash(serverInfo.getID(), j);
      RingElement curElement = new RingElement(position, serverInfo);

      RingElement validPosition = consistentRing.ceiling(curElement);
      if (validPosition == null || !validPosition.equals(curElement)) {
        // There are no server currently assigned to this position
        // So, we can assign current server here
        curServerPositions.add(position);
      }
    }

    if (servers.size() == 1) {
      // It is the first server in our DISTRIBUTED SYSTEM
    } else {
      // take the load from the neighbouring servers
      for (Integer position : curServerPositions) {
        RingElement previousServer = consistentRing.lower(new RingElement(position, null));
        if (previousServer == null) {
          // The last server is handling the request
          previousServer = consistentRing.last();
        }

        // TODO: Handle the case where we take the load from same server twice
        RingElement nextServer = consistentRing.higher(new RingElement(position, null));
        if (nextServer == null) {
          // The next server is after the position 'MOD-1' in the ring
          nextServer = consistentRing.first();
          // from the side [L, MOD-1] and from [0, R]
          // TODO: modify the logic server side to exclude the right end i.e [L, R)
          this.reDistributeLoad(previousServer.getServerInfo(), serverInfo, position, MOD);
          this.reDistributeLoad(previousServer.getServerInfo(), serverInfo, 0, nextServer.getPosition());
        } else {
          this.reDistributeLoad(previousServer.getServerInfo(), serverInfo, position, nextServer.getPosition());
        }

      }
    }

    // Add all the virtual server location in CONSISTENT RING
    for (Integer position : curServerPositions) {
      consistentRing.add(new RingElement(position, serverInfo));
    }
    // Store the positions of the current server in the RING
    serverPosition.put(serverInfo.getID(), curServerPositions);
  }

  // // remove the server having id "serverID"
  // public void removeServer(int serverID) {
  // List<Integer> serverPositions = serverPosition.get(serverID);
  // for (Integer position : serverPositions) {
  // consistentRing.remove(new RingElement(position, serverID));
  // }
  // // TODO: add the logic of distributing the load among the active servers
  // serverPosition.remove(serverID);
  // }

  // get the serverID which is reponsible for the mentioned "key"
  public ServerInfo getCorrectServer(int key) {
    int positionInRing = fastPow(3, key);
    RingElement serverHavingKey = consistentRing.ceiling(new RingElement(positionInRing, null));
    if (serverHavingKey == null) {
      if (consistentRing.isEmpty())
        return null;
      return consistentRing.first().getServerInfo();
    } else
      return serverHavingKey.getServerInfo();
  }

  // Listen to a particular decided port for incoming request
  // And then create LoadBalancerTaks and delegate the request to them for
  // handling
  public void initiate() {
    try {
      ServerSocket server = new ServerSocket(this.PORT);

      while (true) {
        try {
          Socket request = server.accept();

          DataInputStream inputStream = new DataInputStream(request.getInputStream());
          // Delegate the request to a new thread
          new LoadBalancerTaskHandler(this, inputStream.readUTF()).start();
          inputStream.close();

        } catch (Exception e) {
          // Will be here when there is some error with connecting to client
          // TODO: update this in future if required

        }
      }

    } catch (IOException e) {
      // TODO update this if required in future
      // Will be here when unable to listen to a port
      e.printStackTrace();
    }
  }

  public void debugInfo() {
    System.out.println("*******************************************************");
    System.out.print("Ring: ");
    for (RingElement ringElement : consistentRing) {
      System.out.print(ringElement + " ");
    }
    System.out.println();

    System.out.println("Mapping: ");
    for (Integer key : serverPosition.keySet()) {
      System.out.print("(" + key + ": ");
      for (Integer position : serverPosition.get(key)) {
        System.out.print(position + " ");
      }
      System.out.print("), ");
    }
    System.out.println();
    System.out.println("*******************************************************");
  }

  public static void main(String args[]) {

    /**
     * PARAMETERS TO BE PASSED AS COMMAND LINE ARGUMENTS
     * 
     * IP => IP address of the LoadBalancer
     * 
     * PORT => PORT on which LoadBalancer listens for request
     */

    try {
      LoadBalancer loadBalancer = new LoadBalancer(args[0], Integer.valueOf(args[1]));
      loadBalancer.initiate();
    } catch (NumberFormatException e) {
      // TODO: change this if required in future
      e.printStackTrace();
    }
  }
}