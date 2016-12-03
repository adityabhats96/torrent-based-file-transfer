package backend.peer;

import backend.tracker.TrackerThread;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JTextArea;

/**
 *
 * @author hexbioc
 */
public class PeerThread implements Runnable {

    private static final int SINGLE_PART = -1;
    private static final int FINAL_FILE_PART = -2;
    private static final int BUFFER_SIZE = 512;

    private final DataInputStream disPeer;
    private final DataOutputStream dosPeer;
    private final Socket peer;
    private final String fileName;
    private final File newFile;
    private final int filePart;
    private final long offset;
    private final long requiredBytes;

    public PeerThread(Socket peer, String fileName, int filePart, long offset, long requiredBytes) throws IOException {

        this.peer = peer;
        this.fileName = fileName;
        this.filePart = filePart;
        this.offset = offset;
        this.requiredBytes = requiredBytes;

        disPeer = new DataInputStream(peer.getInputStream());
        dosPeer = new DataOutputStream(peer.getOutputStream());

        if (this.filePart == SINGLE_PART) {
            newFile = new File(fileName);
        } else {
            newFile = (this.filePart == FINAL_FILE_PART) ? new File(fileName + "_end") : new File(fileName + "_" + filePart);
        }
        
        System.out.println("New peer thread created. Details:");
        System.out.println(this.toString());
    }

    @Override
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        sb.append("Peer address\t\t: ").append(peer.getInetAddress().getHostAddress());
        sb.append("\nFile Part\t\t: ").append(newFile.getName());
        sb.append("\nFile Part Path\t\t: ").append(newFile.getAbsolutePath());
        sb.append("\nBytes to receive\t: ").append(requiredBytes);
        sb.append("\nOffset\t\t\t: ").append(offset).append("\n");
        
        return sb.toString();
    }
    
    @Override
    public void run() {

        try {
            //Send the name of the required file to the peer
            dosPeer.writeUTF(fileName);
            //Send the offset value
            dosPeer.writeLong(offset);
            //Send the number of required bytes
            dosPeer.writeLong(requiredBytes);

            //Receive the file
            FileOutputStream fos = new FileOutputStream(newFile);
            int bytesRead;
            byte buf[] = new byte[BUFFER_SIZE];
            
            while( (bytesRead = disPeer.read(buf)) > 0 ) {
                fos.write(buf, 0, bytesRead);
            }
            
            //Close streams
            fos.close();
            disPeer.close();
            dosPeer.close();
            peer.close();

        } catch (IOException ex) {
            Logger.getLogger(TrackerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
