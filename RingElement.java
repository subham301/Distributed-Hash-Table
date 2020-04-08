
public class RingElement implements Comparable<RingElement> {
  private int position;
  private ServerInfo serverInfo;

  public RingElement(int position, ServerInfo serverInfo) {
    this.position = position;
    this.serverInfo = serverInfo;
  }

  public int getPosition() {
    return position;
  }

  public ServerInfo getServerInfo() {
    return serverInfo;
  }

  @Override
  public int compareTo(RingElement other) {
    if (this.position == other.getPosition()) {
      return 0;
    }
    if (this.position < other.getPosition()) {
      return -1;
    }
    return 1;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RingElement) {
      RingElement curElement = (RingElement) obj;
      return (this.position == curElement.getPosition());
    }
    return false;
  }

  @Override
  public String toString() {
    return ("(" + this.position + " " + this.serverInfo + ")");
  }

}