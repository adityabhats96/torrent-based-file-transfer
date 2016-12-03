/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package backend.tracker;

import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author hexbioc
 */
public class SharedData {

    private final HashMap<String, HashSet<String>> fileOwnershipData;
    private final HashMap<String, Long> fileSizeData;
    private String requestedFile;
    private String askingPeer;
    private boolean requestAvailable;
    private int peerCount;
    private final Object writeLock;
    private final Object counterLock;

    public SharedData() {

        fileOwnershipData = new HashMap<>();
        fileSizeData = new HashMap<>();
        requestedFile = "";
        askingPeer = "";
        requestAvailable = false;
        peerCount = 0;

        writeLock = new Object();
        counterLock = new Object();
    }

    public boolean addFileInfo(String fileOwner, String fileName, long fileSize) {

        //Add a lock so that no two threads write at the same time
        synchronized (writeLock) {
            if (!fileOwnershipData.containsKey(fileName)) {

                //Create a new HashSet to store IPs of all owners of the required file
                fileOwnershipData.put(fileName, new HashSet<>());
                //Add the file size info
                fileSizeData.put(fileName, fileSize);
            }
            //Add the IP of the owner of the file
            return ((HashSet<String>) fileOwnershipData.get(fileName)).add(fileOwner);
        }
    }
    
    public HashSet<String> getFileOwners(String fileName) {
        return fileOwnershipData.get(fileName);
    }
    
    public int getPeerCount(String fileName) {
        if (fileOwnershipData.containsKey(fileName)) {
            int count;
            count = ((HashSet<String>) fileOwnershipData.get(fileName)).size();
            return count;
        } else {
            return 0;
        }
    }

    public long getFileSize(String fileName) {
        if (fileSizeData.containsKey(fileName)) {
            return fileSizeData.get(fileName);
        } else {
            return 0;
        }
    }

    public boolean isNewRequestAvailable() {
        return requestAvailable;
    }

    public boolean setFileRequest(String fileName, String peerInetAddress, int peerCount) {

        //Method not yet synchronized; assumption that method won't be concurrently called
        if (this.peerCount == 0) {
            requestedFile = fileName;
            askingPeer = peerInetAddress;
            requestAvailable = true;
            this.peerCount = peerCount;
            System.out.println("New file request set. Details:");
            System.out.println("File Name\t: " + requestedFile);
            System.out.println("Asking Peer\t: " + askingPeer);
            System.out.println("Peer Count\t:" + peerCount);
            return true;
        } else {
            return false;
        }
    }

    public void decrementPeerCount() {

        //Add a lock so that counter is decremented safely
        synchronized (counterLock) {
            --peerCount;
            System.out.println("Counter decremented. New value: " + peerCount);
            if (peerCount == 0) {
                requestAvailable = false;
            }
        }
    }

    public String getRequestedFile() {
        return requestedFile;
    }

    public String getAskingPeer() {
        return askingPeer;
    }
}
