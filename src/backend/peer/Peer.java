package backend.peer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Peer {

    private static final int TRACKER_PORT = 8083;
    private static final int PEER_RECEIVER_PORT = 8081;
    private static final int BUFFER_SIZE = 512;
    //Setting the minimum file size to 1 MegaByte
    private static final long MINIMUM_FILE_SIZE_BYTES = 1024 * 1024 * 1L;

    /*TRACKER COMMANDS*/
    //Ask peer to send the list of files it has on its local storage
    private static final byte SEND_FILE_LIST = 1;
    //Get the status of the connected peer (IDLE/FILE_LIST_UPDATED/FILE_REQUIRED/TERMINATE)
    private static final byte SEND_PEER_STATUS = 2;
    //Ask the peer to establish connection to the peer at the IP that is sent following the command
    private static final byte COMMUNICATE_WITH_PEER = 3;
    /*PEER REPLIES TO TRACKER*/
    //Peer is idle
    private static final byte STATE_IDLE = 0;
    //Peer's file list is updated
    private static final byte STATE_FILE_LIST_UPDATED = 1;
    //Peer requires a file; reply will be followed by the file name
    private static final byte STATE_FILE_REQUIRED = 2;
    //Peer wishes to terminate connection; remove peers IP from the fileTable entries and close the socket
    private static final byte STATE_TERMINATE = 3;
    /*TRACKER REPLIES TO PEER*/
    private static final byte FILE_AVAILABLE = 1;
    //private static final byte FILE_NOT_AVAILABLE = 0;

    private String trackerInetAddress;
    private Socket trackerSocket;
    private DataOutputStream dosTracker;
    private DataInputStream disTracker;
    private String fileListName;
    private File fileList;
    private HashMap<String, String> pathMap;
    private String requiredFileName;
    private boolean fileListUpdatedFlag;
    private boolean fileRequiredFlag;
    private boolean terminateFlag;

    public Peer() {

        trackerInetAddress = null;

        if (System.getProperty("os.name").contains("Linux")) {
            fileListName = System.getProperty("user.home") + "/.filetransfer/file_list.txt";
        } else if (System.getProperty("os.name").contains("Windows")) {
            fileListName = System.getProperty("user.home") + "\\.filetransfer\\file_list.txt";
        }

        trackerSocket = null;
        disTracker = null;
        dosTracker = null;
        fileListUpdatedFlag = true;
        fileRequiredFlag = false;
        terminateFlag = false;
        requiredFileName = null;

        fileList = new File(fileListName);
        if (!fileList.exists()) {
            pathMap = new HashMap<>();
        } else {

            try {
                pathMap = (HashMap<String, String>) new ObjectInputStream(new FileInputStream(fileList)).readObject();

            } catch (IOException | ClassNotFoundException ex) {
                Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public HashMap<String, String> getPathMap() {
        return pathMap;
    }

    
    public void setTrackerInetAddress(String trackerInetAddress) {

        this.trackerInetAddress = trackerInetAddress;
    }

    public void addToFileList(File file) {
        //Update local path map
        pathMap.put(file.getName(), file.getAbsolutePath());
    }

    public void addToFileList(String filePath) {
        File file = new File(filePath);
        addToFileList(file);
    }
    
    public void terminate() {
        terminateFlag = true;
    }

    public void connectToTracker() {
        try {
            trackerSocket = new Socket(trackerInetAddress, TRACKER_PORT);

            dosTracker = new DataOutputStream(trackerSocket.getOutputStream());
            disTracker = new DataInputStream(trackerSocket.getInputStream());

        } catch (IOException ex) {
            Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public void communicateWithTracker() {

        boolean loopFlag = true;

        while (loopFlag) {
            try {
                byte command = (byte) disTracker.read();

                switch (command) {
                    case SEND_FILE_LIST: {
                        sendFileList();
                        break;
                    }
                    case SEND_PEER_STATUS: {

                        if (terminateFlag) {
                            dosTracker.write(STATE_TERMINATE);
                            this.terminateAllConnections();
                            loopFlag = false;

                        } else if (fileRequiredFlag) {
                            dosTracker.write(STATE_FILE_REQUIRED);

                            //Send the name of the required file
                            dosTracker.writeUTF(requiredFileName);
                            //Wait for tracker's reply on file availability
                            if (disTracker.read() == FILE_AVAILABLE) {
                                //Tracker will follow up with number of IPs and file size in bytes
                                int peerCount = disTracker.readInt();
                                long fileSize = disTracker.readLong();
                                receiveFile(peerCount, fileSize);
                            }

                        } else if (fileListUpdatedFlag) {
                            dosTracker.write(STATE_FILE_LIST_UPDATED);

                        } else {
                            dosTracker.write(STATE_IDLE);
                        }
                        break;
                    }
                    case COMMUNICATE_WITH_PEER: {

                        //Another peer requires a file that is available here
                        //Receive the IP of the peer to connect to
                        String peerInetAddress = disTracker.readUTF();

                        communicateWithPeer(peerInetAddress);
                        break;
                    }

                }
            } catch (IOException ex) {
                Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void sendFileList() {

        System.out.println("Sending file list...");
        long fileSize = 0L;
        try {
            for (String fileName : pathMap.keySet()) {

                //Send the file name
                dosTracker.writeUTF(fileName);
                //Send the file size
                fileSize = new File((String) pathMap.get(fileName)).length();
                dosTracker.writeLong(fileSize);
            }
            //Denote the end of file name transfer
            dosTracker.writeUTF("end");
        } catch (IOException ex) {
            Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
        }

        fileListUpdatedFlag = false;
    }

    public void setFileRequired(String requiredFileName) {

        this.requiredFileName = requiredFileName;
        fileRequiredFlag = true;
    }

    public void receiveFile(int peerCount, long fileSize) {

        System.out.printf("Waiting for peers to connect; total: %d%nFilesize: %d%n", peerCount, fileSize);
        try {
            //Start a server socket
            ServerSocket peerServerSocket = new ServerSocket(PEER_RECEIVER_PORT);
            //Decide the required size of data block from each peer
            if (fileSize < MINIMUM_FILE_SIZE_BYTES) {
                new Thread(new PeerThread(peerServerSocket.accept(), requiredFileName, -1, 0, fileSize)).start();

            } else {
                long offset = 0;
                long blockSize = fileSize / peerCount;
                Thread threadSet[] = new Thread[peerCount];

                for (int i = 0; i < peerCount - 1; i++) {
                    threadSet[i] = new Thread(new PeerThread(peerServerSocket.accept(), requiredFileName, i, offset, blockSize));
                    threadSet[i].start();
                    offset += blockSize;
                }
                //Get the rest of the file from the last peer
                threadSet[peerCount - 1] = new Thread(new PeerThread(peerServerSocket.accept(), requiredFileName, -2, offset, fileSize - offset));
                threadSet[peerCount - 1].start();
                //Wait till all threads have received the file
                for (Thread t : threadSet) {
                    try {
                        t.join();
                    } catch (InterruptedException ex) {
                        Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }

                threadSet = null;

                //Now combine all these files
                File receivedFile = new File(requiredFileName);
                FileOutputStream mainFOS = new FileOutputStream(receivedFile, true);
                FileInputStream mainFIS = null;
                File tempFile;
                byte buf[] = new byte[BUFFER_SIZE];
                int bytesRead;
                
                for (int i = 0; i < peerCount; i++) {

                    tempFile = (i == peerCount - 1) ? new File(requiredFileName + "_end") : new File(requiredFileName + "_" + i);
                    mainFIS = new FileInputStream(tempFile);
                    
                    while ((bytesRead = mainFIS.read(buf)) > 0) {
                        mainFOS.write(buf, 0, bytesRead);
                    }
                    mainFIS.close();
                    //Delete the temporarily generated file
                    //tempFile.delete();
                }
                mainFOS.close();
            }

            System.out.println("File successfully received!");
            fileRequiredFlag = false;
            peerServerSocket.close();
        } catch (IOException ex) {
            Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }

    public void communicateWithPeer(String peerInetAddress) {

        try {
            Socket peerSocket = new Socket(peerInetAddress, PEER_RECEIVER_PORT);
            DataInputStream disPeer = new DataInputStream(peerSocket.getInputStream());
            DataOutputStream dosPeer = new DataOutputStream(peerSocket.getOutputStream());
            /*
             After connection to peer, the peer will send the following details
             sequentially:
             1.  Required file name
             2.  Offset
             3.  Number of bytes required from the offset
             */

            //Get the required file name
            String fileName = disPeer.readUTF();
            //Get the offset value
            long offset = disPeer.readLong();
            //Get the required number of bytes
            long requiredBytes = disPeer.readLong();

            long sentBytes = 0;
            byte[] buffer = new byte[BUFFER_SIZE];

            //Open the required File as a random access file
            RandomAccessFile randomFile = new RandomAccessFile((String) pathMap.get(fileName), "r");

            //Start reading and sending the bytes till required number of bytes is reached
            randomFile.seek(offset);
            while (sentBytes < (requiredBytes - BUFFER_SIZE)) {
                randomFile.readFully(buffer);
                dosPeer.write(buffer);
                sentBytes += BUFFER_SIZE;
            }
            buffer = new byte[(int) (requiredBytes - sentBytes)];
            randomFile.readFully(buffer);
            dosPeer.write(buffer);

            //Close all streams and terminate connection
            randomFile.close();
            dosPeer.close();
            disPeer.close();
            peerSocket.close();

        } catch (IOException ex) {
            Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void terminateAllConnections() {
        try {
            disTracker.close();
            dosTracker.close();
            trackerSocket.close();

            //Write the pathMap to the file_list.txt file
            new ObjectOutputStream(new FileOutputStream(fileList)).writeObject(pathMap);

        } catch (IOException ex) {
            Logger.getLogger(Peer.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static void main(String args[]) {

        Peer p = new Peer();
        p.setTrackerInetAddress("192.168.43.203");
        //p.addToFileList("D:\\Works\\NITK Curriculum Projects\\Semester IV\\DAA\\Paper Presentation\\source30.zip");
        p.setFileRequired("x.zip");
        p.connectToTracker();
        p.communicateWithTracker();
    }
}
