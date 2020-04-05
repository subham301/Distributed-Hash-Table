
public class RingElement implements Comparable<RingElement> {
  private int position;
  private int serverID;

  public RingElement(int position, int serverID) {
    this.position = position;
    this.serverID = serverID;
  }

  public int getPosition() {
    return position;
  }

  public int getServerID() {
    return serverID;
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
    // TODO Auto-generated method stub
    return ("(" + this.position + " " + this.serverID + ")");
  }

}