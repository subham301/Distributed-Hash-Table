import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;

public class TestServer extends Thread {
  private DataInputStream dis;

  public TestServer(DataInputStream dis) {
    this.dis = dis;
  }

  @Override
  public void run() {
    try {
      String message = (String) dis.readUTF();
      while (!message.equals("DONE!")) {
        System.out.println("At server: '" + message + "'");
        message = (String) dis.readUTF();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String args[]) {
    try {
      ServerSocket ss = new ServerSocket(10236);

      while (true) {
        Socket client = ss.accept();
        DataInputStream dis = new DataInputStream(client.getInputStream());
        new TestServer(dis).start();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

/**
 * CLIENT CODE for testing -->
 * 
 * Socket s = new Socket("localhost", 10236); DataOutputStream dout = new
 * DataOutputStream(s.getOutputStream());
 * 
 * BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
 * String message = br.readLine(); while (!message.equals("OVER!")) {
 * dout.writeUTF(message); dout.flush(); message = br.readLine(); }
 * dout.writeUTF("DONE!");
 * 
 * s.close();
 */