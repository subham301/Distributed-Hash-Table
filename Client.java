import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;

public class Client {
  public static void main(String args[]) {
    // <CLIENT_IP> <CLIENT_PORT> <LOAD_BALANCER_IP> <LOAD_BALANCER_PORT>

    String clientIP;
    String loadBalancerIP;
    int clientPort = -1, loadBalancerPort = -1;

    try {
      if (args.length != 4)
        throw new NumberFormatException();

      clientIP = args[0];
      loadBalancerIP = args[2];

      clientPort = Integer.valueOf(args[1]);
      loadBalancerPort = Integer.valueOf(args[3]);
    } catch (NumberFormatException e) {
      System.out.println("Invalid arguments. Please provide the arguments in the form --> ");
      System.out.println("<CLIENT_IP> <CLIENT_PORT> <LOAD_BALANCER_IP> <LOAD_BALANCER_PORT>");
      return;
    }

    try {
      /**
       * *********************************************************************************
       * 
       * CLIENT <SENDER_IP> <SENDER_PORT> <OPERATION> ==> <OPERATION> ==>
       * 
       * GET <KEY>
       * 
       * PUT <KEY> <VALUE>
       * 
       * ***********************************************************************************
       */
      System.out.println("Enter the operation you want to perform -> ");
      System.out.println("1. PUT\n2. GET\n3. Exit");
      System.out.print("Enter your choice and press enter: ");

      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      String option = reader.readLine();
      int operation = Integer.valueOf(option);

      while (operation != 3) {
        if (operation != 1 && operation != 2) {
          System.out.println("Invalid choice! Try again...\n");
        } else {
          Socket socket = new Socket(loadBalancerIP, loadBalancerPort);

          // build the <TASK> message
          String task = "CLIENT" + " " + clientIP + " " + clientPort + " ";
          if (operation == 1) {
            System.out.print("Enter the key to insert and press enter: ");
            String key = reader.readLine();
            System.out.print("Enter the value and press enter: ");
            String value = reader.readLine();

            try {
              task += "PUT" + " ";
              task += Integer.valueOf(key) + " " + Integer.valueOf(value);
            } catch (NumberFormatException e) {
              System.out.println("Invalid choice! Try again...\n");
            }
          } else {
            System.out.print("Enter the key and press enter: ");
            String key = reader.readLine();

            try {
              task += "GET" + " ";
              task += Integer.valueOf(key);
            } catch (NumberFormatException e) {
              System.out.println("Invalid choice! Try again...\n");
            }
          }

          DataOutputStream out = new DataOutputStream(socket.getOutputStream());
          out.writeUTF(task);
          out.flush();
          out.close();

          socket.close();

          // wait for the response
          ServerSocket response = new ServerSocket(clientPort);
          socket = response.accept();
          DataInputStream in = new DataInputStream(socket.getInputStream());
          System.out.println("RESPONSE: " + in.readUTF());
          in.close();
          response.close();
        }

        // Take the user's choice
        System.out.println("Enter the operation you want to perform -> ");
        System.out.println("1. PUT\n2. GET\n3. Exit");
        System.out.print("Enter your choice and press enter: ");

        option = reader.readLine();
        operation = Integer.valueOf(option);
      }
    } catch (NumberFormatException e) {
      System.out.println("No such option found!");
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}