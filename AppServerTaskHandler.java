import java.io.DataOutputStream;
import java.net.Socket;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

class AppServerTaskHandler extends Thread {
  private String task;
  private String destinationIP;
  private int destinationPort;
  private AppServer appServer;

  public AppServerTaskHandler(String task, String destinationIP, int destinationPort, AppServer server) {
    this.task = task;
    this.destinationIP = destinationIP;
    this.destinationPort = destinationPort;
    // This server instance is shared between all the Tasks created by that
    // AppServer
    this.appServer = server;
  }

  private void sendResponse(String message) {
    try {
      Socket socket = new Socket(this.destinationIP, this.destinationPort);

      DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
      dout.writeUTF(message);
      dout.flush();

      socket.close();
    } catch (Exception e) {
      // TODO: Write the error to the log file
      e.printStackTrace();
    }

  }

  @Override
  public void run() {
    task.trim();
    try {
      String[] parameters = task.split(" ");

      /**
       * ***********************************************************************************
       * GET THE VALUE OF THE MENTIONED KEY ==>
       * 
       * GET <KEY>
       * 
       * ==> respond to the client with message containing the "<KEY> <VALUE>"
       * 
       * ***********************************************************************************
       * PUT THE MENTIONED <KEY VALUE> PAIR IN THE STORE ==>
       * 
       * PUT <KEY> <VALUE>
       * 
       * ==> respond to the client with a success message
       * 
       * ***********************************************************************************
       * PUT ALL THE <KEY VALUE> PAIRS IN THE STORE ==>
       * 
       * PUT_ALL <KEY1> <VALUE1> <KEY2> <VALUE2> <KEY3> <VALUE3> .... <KEYN> <VALUEN>
       * 
       * ==> no response
       * 
       * ***********************************************************************************
       * Remove the key in range [FROM_KEY, TO_KEY] from current server and add it to
       * the <DESTINATION_SERVER>
       * 
       * RE_DISTRIBUTE <DESTINATION_SERVERINFO> <FROM_KEY> <TO_KEY>
       * 
       * ==> <SERVERINFO> ==> <IP> <PORT> <SERVER_ID>
       * 
       * ==> send the response to DESTINATION_SERVER (PUT_ALL) to put all the
       * key,values
       * ***********************************************************************************
       */
      /**
       * PUT <KEY> <VALUE> ==> respond to the client with a success message
       */
      if (parameters[0].compareToIgnoreCase("PUT") == 0) {
        int key = Integer.valueOf(parameters[1]);
        int value = Integer.valueOf(parameters[2]);
        this.appServer.put(key, value);
        this.sendResponse("The <" + key + ", " + value + "> pair is successfully stored!");
      }

      /**
       * GET <KEY> ==> respond to the client with message containing the "<KEY>
       * <VALUE>"
       */
      else if (parameters[0].compareToIgnoreCase("GET") == 0) {
        int key = Integer.valueOf(parameters[1]);
        int value = this.appServer.get(key);
        this.sendResponse(key + " " + value);
      }

      /**
       * PUT_ALL <KEY1> <VALUE1> <KEY2> <VALUE2> <KEY3> <VALUE3> .... <KEYN> <VALUEN>
       */
      else if (parameters[0].compareToIgnoreCase("PUT_ALL") == 0) {
        if (parameters.length % 2 != 1) {
          // The message is not well formed
          throw new IndexOutOfBoundsException();
        }
        System.out.println(task);
        ConcurrentNavigableMap<Integer, Integer> store = new ConcurrentSkipListMap<>();

        // build up the store with the <key, value> pair
        for (int j = 1; j < parameters.length; j += 2) {
          int key = Integer.valueOf(parameters[j]);
          int value = Integer.valueOf(parameters[j + 1]);
          store.put(key, value);
        }

        this.appServer.putKeyValues(store);
      }

      /**
       * RE_DISTRIBUTE <FROM_KEY> <TO_KEY>
       * 
       */
      else if (parameters[0].compareToIgnoreCase("RE_DISTRIBUTE") == 0) {
        // get all the <KEY, VALUE> pairs from the current server
        ConcurrentNavigableMap<Integer, Integer> myMap = this.appServer
            .getKeyValueInRange(Integer.valueOf(parameters[1]), Integer.valueOf(parameters[2]));

        NavigableSet<Integer> keys = myMap.keySet();

        // remove the keys from the current server
        this.appServer.removeKeyValues(keys);

        // prepare the message to be send to the DESTINATION_SERVER
        // MESSAGE FORMAT ==> <DEST_IP> <DEST_PORT> <TASK => <OPERATION PARAMETERS> >
        String responseMessage = "NO_IP" + " " + "0" + " " + "PUT_ALL";
        for (Integer key : keys) {
          responseMessage += " " + key + " " + myMap.get(key);
        }
        // send the message to the DESTINATION_SERVER
        this.sendResponse(responseMessage.trim());
      }

      else {
        // Neither 'put' nor 'get'
        // INVALID OPERATION !!
        throw new IndexOutOfBoundsException();
      }
    } catch (IndexOutOfBoundsException e) {
      // INVALID OPERATION
      // this.sendResponse("Invalid Request! Please check your request message and try
      // again!");
      e.printStackTrace();
    } catch (NumberFormatException e) {
      // INVALID OPERATION
      // this.sendResponse("Invalid Request! Please check your request message and try
      // again!");
      e.printStackTrace();
    }
  }

}