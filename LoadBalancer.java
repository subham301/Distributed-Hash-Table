import java.util.*;

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
  private TreeSet<RingElement> consistentRing;
  // Store the list of position to which a particular server is mapped
  private Map<Integer, List<Integer>> serverPosition;
  // TODO: remove this using SOCKETS
  private Map<Integer, AppServer> servers;

  public LoadBalancer() {
    consistentRing = new TreeSet<>();
    serverPosition = new HashMap<>();
    // TODO: update it accordingly
    servers = new HashMap<>();
  }

  // add the new server having id "serverID"
  public void addServer(int serverID) {
    // TODO: change the next line accordingly when using SOCKETS
    servers.put(serverID, new AppServer(serverID));
    List<Integer> curServerPositions = new ArrayList<>();

    for (int j = 1; j <= HASHES; j++) {
      int position = getHash(serverID, j);
      RingElement curElement = new RingElement(position, serverID);

      RingElement validPosition = consistentRing.ceiling(curElement);
      if (validPosition == null || !validPosition.equals(curElement)) {
        // There are no server currently assigned to this position
        // So, we can assign current server here
        curServerPositions.add(position);
        consistentRing.add(curElement);
      }
    }
    // TODO: take the load from the neighbouring servers
    serverPosition.put(serverID, curServerPositions);
  }

  // remove the server having id "serverID"
  public void removeServer(int serverID) {
    List<Integer> serverPositions = serverPosition.get(serverID);
    for (Integer position : serverPositions) {
      consistentRing.remove(new RingElement(position, serverID));
    }
    // TODO: add the logic of distributing the load among the active servers
    serverPosition.remove(serverID);
  }

  // get the serverID which is reponsible for the mentioned "key"
  public int getCorrectServer(int key) {
    int positionInRing = fastPow(3, key);
    RingElement serverHavingKey = consistentRing.ceiling(new RingElement(positionInRing, -1));
    if (serverHavingKey == null) {
      if (consistentRing.isEmpty())
        return -1;
      return consistentRing.first().getServerID();
    } else
      return serverHavingKey.getServerID();
  }

  // TODO: write the logic to contact the correct server and put the keys
  public void put(int key, int value) {
    int serverID = getCorrectServer(key);
    servers.get(serverID).put(key, value);
  }

  // TODO: write the logic to contact the correct server and get the key
  public int get(int key) {
    int serverID = getCorrectServer(key);
    return servers.get(serverID).get(key);
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
    LoadBalancer loadBalancer = new LoadBalancer();

    loadBalancer.debugInfo();
    loadBalancer.addServer(2);
    loadBalancer.debugInfo();
    loadBalancer.addServer(3);
    loadBalancer.debugInfo();
    loadBalancer.addServer(4);
    loadBalancer.debugInfo();

    System.out.println("Server position for key(5): " + loadBalancer.getCorrectServer(5));
    System.out.println("Server position for key(1): " + loadBalancer.getCorrectServer(1));
    System.out.println("Server position for key(0): " + loadBalancer.getCorrectServer(0));
    System.out.println("Server position for key(12): " + loadBalancer.getCorrectServer(12));

    // loadBalancer.removeServer(3);
    // loadBalancer.debugInfo();

    // System.out.println("Server position for key(5): " +
    // loadBalancer.getCorrectServer(5));
    // System.out.println("Server position for key(1): " +
    // loadBalancer.getCorrectServer(1));
    // System.out.println("Server position for key(0): " +
    // loadBalancer.getCorrectServer(0));
    // System.out.println("Server position for key(12): " +
    // loadBalancer.getCorrectServer(12));

    for (int j = 1; j <= 30; j++) {
      System.out.println("Putting " + j + " in server id: " + loadBalancer.getCorrectServer(j));
      loadBalancer.put(j, j * 10 + 1);
    }

    for (int j = 1; j <= 30; j++) {
      System.out.println(j + ": " + loadBalancer.get(j));
    }
  }
}