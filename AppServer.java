import java.util.*;

public class AppServer {
  public static final int KEY_NOT_PRESENT = -1;

  private Map<Integer, Integer> hashMap;
  private final int SERVER_ID;

  public AppServer(int serverID) {
    hashMap = new HashMap<>();
    SERVER_ID = serverID;
  }

  public int getServerId() {
    return SERVER_ID;
  }

  public void put(int key, int value) {
    if (hashMap.containsKey(key)) {
      hashMap.replace(key, value);
      return;
    }
    hashMap.put(key, value);
  }

  public int get(int key) {
    if (hashMap.containsKey(key)) {
      return hashMap.get(key);
    }
    return KEY_NOT_PRESENT;
  }
}