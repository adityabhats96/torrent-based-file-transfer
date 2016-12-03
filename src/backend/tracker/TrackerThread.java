package backend.tracker;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TrackerThread implements Runnable {

    /*TRACKER COMMAND LIST*/
    //Ask peer to send the list of files it has on its local storage
    private static final byte GET_FILE_LIST = 1;
    //Get the status of the connected peer (IDLE/FILE_LIST_UPDATED/FILE_REQUIRED/TERMINATE)
    private static final byte GET_PEER_STATUS = 2;
    //Ask the peer to connect to the peer at the IP that is sent following the command
    private static final byte COMMUNICATE_WITH_PEER = 3;
    /*PEER REPLIES TO TRACKER*/
    //Peer is idle
    private static final byte PEER_REPLY_IDLE = 0;
    //Peer's file list is updated
    private static final byte PEER_REPLY_FILE_LIST_UPDATED = 1;
    //Peer requires a file; reply will be followed by the file name
    private static final byte PEER_REPLY_FILE_REQUIRED = 2;
    //Peer wishes to terminate connection; remove peers IP from the fileTable entries and close the socket
    private static final byte PEER_REPLY_TERMINATE = 3;
    /*TRACKER REPLIES*/
    private static final byte FILE_AVAILABLE = 1;
    private static final byte FILE_NOT_AVAILABLE = 0;

    private final Socket peerSocket;
    private final int threadNumber;
    private final String peerInetAddress;
    private final DataOutputStream dos;
    private final DataInputStream dis;
    private final SharedData sharedData;

    public TrackerThread(Socket socket, SharedData sharedData, int threadNumber) throws IOException {
        this.peerSocket = socket;
        this.threadNumber = threadNumber;
        this.sharedData = sharedData;

        peerInetAddress = peerSocket.getInetAddress().getHostAddress();
        dos = new DataOutputStream(peerSocket.getOutputStream());
        dis = new DataInputStream(peerSocket.getInputStream());
    }

    public void sendCommand(byte command) {

        try {
            dos.write(command);
           // System.out.printf("Sent command to peer %s\t: %d%n", peerInetAddress, command);
        } catch (IOException ex) {
            Logger.getLogger(TrackerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void receiveFileList() {
        String fileName = null;
        long fileSize = 0L;

        try {
            while (!(fileName = dis.readUTF()).equals("end")) {
                fileSize = dis.readLong();
                sharedData.addFileInfo(peerInetAddress, fileName, fileSize);
            }

        } catch (IOException ex) {
            Logger.getLogger(TrackerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void terminateConnection() throws IOException {

        /* Add code to remove peers entries from the shared data here*/
        dis.close();
        dos.close();
        peerSocket.close();
    }

    @Override
    public void run() {
        try {
            //System.out.println("Something here soon...");
            dos.write(GET_FILE_LIST);
            receiveFileList();

            byte reply;
            boolean terminateFlag = false;
            while (!terminateFlag) {
                sendCommand(GET_PEER_STATUS);
                
                reply = (byte) dis.read();
                switch (reply) {
                    
                    case PEER_REPLY_IDLE:
                        //Wait for sometime and continue
                        
                        break;
                        
                    case PEER_REPLY_FILE_LIST_UPDATED:
                        sendCommand(GET_FILE_LIST);
                        receiveFileList();
                        break;
                    
                    case PEER_REPLY_FILE_REQUIRED:
                        //Read the name of the file
                        String requiredFile = dis.readUTF();
                        //Get the number of peers having the file
                        int peerCount = sharedData.getPeerCount(requiredFile);
                        if (peerCount != 0) {
                            //File is available; send status
                            sendCommand(FILE_AVAILABLE);
                            //Follow up by sending the number of peers with file
                            dos.writeInt(peerCount);
                            //Follow up by sending the file size
                            dos.writeLong(sharedData.getFileSize(requiredFile));
                            //Set the required file name in the sharedData
                            while (!sharedData.setFileRequest(requiredFile, peerInetAddress, peerCount));
                        } else {
                            //File is not available
                            sendCommand(FILE_NOT_AVAILABLE);
                        }
                        break;
                    
                    case PEER_REPLY_TERMINATE:
                        //Peer wants to terminate connection; remove peer's entry
                        //from the lists and close socket & streams
                        terminateFlag = true;
                        terminateConnection();
                        break;
                }
                
                //Check if any file is required by another peer
                if(sharedData.isNewRequestAvailable()) {
                    
                    String requestedFile = sharedData.getRequestedFile();
                    String askingPeer = sharedData.getAskingPeer();
                    
                    if(!askingPeer.equals(peerInetAddress)) {
                        //The asking peer is not the currently connected peer
                        //Check if connected peer has requested file
                        HashSet<String> hs = (HashSet<String>)sharedData.getFileOwners(requestedFile);
                        if(hs.contains(peerInetAddress)) {
                            //Currently connected peer has required file
                            //Ask it to communicate with askingPeer
                            sendCommand(COMMUNICATE_WITH_PEER);
                            //Send the InetAddress of the asking peer
                            dos.writeUTF(askingPeer);
                            //Decrement the counter of the shared data
                            sharedData.decrementPeerCount();
                            //Wait till the transfer completes, so that thread does not
                            //attempt another transfer again
                            System.out.println("Waiting until file transfer completes...");
                            while(sharedData.isNewRequestAvailable());
                        }
                    }
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(TrackerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
