
public class ServerInfo {
  private final String IP;
  private final int PORT;
  private final int ID;

  public ServerInfo(String ip, int port, int id) {
    this.IP = ip;
    this.PORT = port;
    this.ID = id;
  }

  public String getIP() {
    return IP;
  }

  public int getPORT() {
    return PORT;
  }

  public int getID() {
    return ID;
  }

  @Override
  public String toString() {
    return ("<" + this.IP + ", " + this.PORT + ", " + this.ID + ">");
  }
}