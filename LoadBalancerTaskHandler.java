import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class LoadBalancerTaskHandler extends Thread {
  private LoadBalancer loadBalancer;
  private String task;

  public LoadBalancerTaskHandler(LoadBalancer loadBalancer, String task) {
    this.loadBalancer = loadBalancer;
    this.task = task;
  }

  @Override
  public void run() {
    /**
     * **********************************************************************************
     * SERVER <SENDER_IP> <SENDER_PORT> <OPERATION> <SERVER_ID> ==> <OPERATION> ==>
     * 
     * ==> <OPERATION> ==> ADD
     * 
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
    try {
      String[] parameters = task.split(" ");
      if (parameters[0].compareToIgnoreCase("SERVER") == 0) {
        ServerInfo fromServer = new ServerInfo(parameters[1], Integer.valueOf(parameters[2]),
            Integer.valueOf(parameters[4]));

        this.loadBalancer.addServer(fromServer);

      } else if (parameters[0].compareToIgnoreCase("CLIENT") == 0) {
        // either putting a <Key, Value> or getting <Value> for a <key>

        // pass the request to the appropraite server to handle it
        // 4th position is the KEY
        ServerInfo correctServer = loadBalancer.getCorrectServer(Integer.valueOf(parameters[4]));

        System.out.println("CORRECT_SERVER: " + correctServer);

        if (correctServer == null) {
          // There is no server available to handle the requests
          assert (false);
        }
        // forward the request to the correctServer
        Socket socket = new Socket(correctServer.getIP(), correctServer.getPORT());

        DataOutputStream outStream = new DataOutputStream(socket.getOutputStream());
        // Message: <CLIENT_IP> <CLIENT_PORT> <TASK => <OPERATION PARAMETERS> >
        String messageToSend = "";
        for (int j = 1; j < parameters.length; j++) {
          messageToSend += parameters[j] + " ";
        }
        outStream.writeUTF(messageToSend);
        outStream.flush();
        outStream.close();

        socket.close();

      } else {
        // INVALID OPERATION
        throw new IndexOutOfBoundsException();
      }
    } catch (IndexOutOfBoundsException e) {
      // TODO: write to logs or something else if needed in future
    } catch (NumberFormatException e) {
      // TODO: write to logs or something else if needed in future
    } catch (UnknownHostException e) {
      // TODO: write to logs or something else if needed in future
    } catch (IOException e) {
      // TODO: write to logs or something else if needed in future
    }
  }
}