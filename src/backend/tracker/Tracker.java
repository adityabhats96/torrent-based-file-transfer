package backend.tracker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Tracker {

    //The port where the Tracker Server will be hosted
    private static final int TRACKER_PORT = 8080;

    //The tracker's ServerSocket
    private ServerSocket serverSocket;
    //Shared data for the threads containing common file information and requests
    private final SharedData sharedData;
    //An integer value to name different threads
    private int threadNumber;
    //A flag to denote that the tracker is terminating so that the socket is closed
    private boolean terminateFlag;

    public Tracker() {
        try {
            serverSocket = new ServerSocket(TRACKER_PORT);
            System.out.println("Tracker started; listening on port " + TRACKER_PORT);
        } catch (IOException ex) {
            Logger.getLogger(Tracker.class.getName()).log(Level.SEVERE, null, ex);
        }
        sharedData = new SharedData();
        threadNumber = 0;
        terminateFlag = false;
    }

    public void begin() {

        Socket peer = null;

        try {
            while (!terminateFlag) {

                //Accept a new connection
                peer = serverSocket.accept();
                System.out.println("Peer connected from " + peer.getInetAddress().getHostAddress() + "\t[PEER " + ++threadNumber + "]");
                //Pass it on to a new thread
                new Thread(new TrackerThread(peer, sharedData, threadNumber)).start();
            }
            //Terminate flag value has changed; close socket
            serverSocket.close();
            
            System.out.println("ServerSocket closed. " + threadNumber + " peers are now literally trackerless.");
        } catch (IOException ex) {
            Logger.getLogger(Tracker.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void terminate() {
        terminateFlag = true;
    }

    public static void main(String[] args) {
        new Tracker().begin();
    }
}
