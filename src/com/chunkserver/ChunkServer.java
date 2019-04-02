package com.chunkserver;

import com.interfaces.ChunkServerInterface;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "/Users/Nataliercraun/Documents/TinyFS-2/db/"; 
	public static long counter;
	
	// Should these all be here/initialized like this??
	
	public static ServerSocket ss; 
	public static int port = 4000; 
	public static Socket s; 
	public static ObjectOutputStream oos;
	public static ObjectInputStream ois; 
	public static int InitializeChunk = 1;
	public static int PutChunk = 2; 
	public static int GetChunk = 3; 

	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		
		String filename = "metadata"; 
        String absoluteFilePath = filePath+filename;
        File file = new File(absoluteFilePath);
		
        
        try {
			if(file.createNewFile()){
			   System.out.println("Creating metadata file");
			} else {
				
		        BufferedReader br = null;
				try {
					br = new BufferedReader(new FileReader(file));
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				String temp = br.readLine();
				if (temp == null) {
					counter = 0; 
				} else {
					counter = Long.valueOf(temp);
				}
				 
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/**
	 * Each chunk corresponds to a file. Return the chunk handle of the last chunk
	 * in the file.
	 */
	public String initializeChunk() {
		
		String filename = "testfile_" + counter;
        String absoluteFilePath = filePath+filename;
        File file = new File(absoluteFilePath);
        
        String metaFilename = "metadata"; 
        String metaFilePath = filePath+metaFilename;
        File metaFile = new File(metaFilePath);
        
        try {
			if(file.createNewFile()) {
			    counter++; 
			    
			    try {
					BufferedWriter bw = new BufferedWriter(new FileWriter(metaFile));
					bw.write(String.valueOf(counter));
					bw.flush();
					bw.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			} else System.out.println("File "+absoluteFilePath+" already exists");
		} catch (IOException e) {
			e.printStackTrace();
		}
        
		return filename;
	}

	/**
	 * Write the byte array to the chunk at the specified offset The byte array size
	 * should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		
		String fullname = filePath + ChunkHandle; 
		
		RandomAccessFile raf = null; 
		try {
			raf = new RandomAccessFile(fullname, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	    try {
			raf.seek(offset);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			raf.write(payload);
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return true;
	}

	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		
		String fullname = filePath + ChunkHandle; 
		byte[] payload = new byte[NumberOfBytes];
		
		RandomAccessFile raf = null; 
		try {
			raf = new RandomAccessFile(fullname, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	    try {
			raf.seek(offset);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	    try {
			raf.read(payload);
		} catch (IOException e) {
			e.printStackTrace();
		}		
		
		
		return payload;
	}
	
	
	public static void startChunkServer() {
		// initialize chunk server 
		ChunkServer cs = new ChunkServer();
		try {
			// start a server socket binding to a port 
			ss = new ServerSocket(port);
			port = ss.getLocalPort();
			System.out.println("Chunk server bound to port " + port);
			
			// write port to metadata file 
			String filename = "portnum"; 
	        String absoluteFilePath = filePath+filename;
	        File file = new File(absoluteFilePath);
	        
	        try {
				if(file.createNewFile()){
				   System.out.println("Creating port number file");
				} 
			} catch (IOException e) {
				e.printStackTrace();
			}
	        
			FileWriter fw = new FileWriter(absoluteFilePath);
			PrintWriter pw = new PrintWriter(fw);
			pw.println(Integer.toString(port));
			pw.close();
		} catch (IOException ioe) {
			System.out.println("ioe in chunkserver binding to port: " + port);
			return; 
		}
		
		
		// keep listening on the port for new connections 
		while (true) {
			try {
				// new client connects to server, create new socket for client 
				// and server to communicate through 
				System.out.println("Waiting for client...");
				s = ss.accept(); // blocking 
				System.out.println("Connection from " + s.getInetAddress());
				oos = new ObjectOutputStream(s.getOutputStream());
				ois = new ObjectInputStream(s.getInputStream());
								
				// keep reading from this socket for future client requests 
				// until client connection is closed 
				while (!s.isClosed()) {
					// read payload size 
					int payloadsize = getPayloadInt(ois);
					if (payloadsize == -1) {
						break; 
					}
					// read command identifier and switch output based on command 
					int command = getPayloadInt(ois);
					if (command == InitializeChunk) {
						// response 
						String chunkHandle = cs.initializeChunk();
						byte[] payload = chunkHandle.getBytes();
						// write chunk handle size 
						oos.writeInt(4 + payload.length);
						// write chunk handle
						oos.write(payload);
						oos.flush();
					} else if (command == PutChunk) {
						System.out.println("Command is PUT chunk");
						// parse parameters 
						int chunkHandleLength = getPayloadInt(ois);
						byte[] chunkHandle = getPayload(ois, chunkHandleLength);
						String handle = new String(chunkHandle).toString();
						int payloadLength = getPayloadInt(ois);
						byte[] payload = getPayload(ois, payloadLength);
						int offset = getPayloadInt(ois);
						// response 
						boolean result = cs.putChunk(handle, payload, offset);
						// write size (int)
						oos.writeInt(4);
						// write result as int: 0 is false, 1 is true 
						if (result == false) {
							oos.writeInt(0);
						} else {
							oos.writeInt(1);
						}
						oos.flush();
					} else if (command == GetChunk) {
						// parse parameters 
						int chunkHandleLength = getPayloadInt(ois);
						byte[] chunkHandle = getPayload(ois, chunkHandleLength);
						String handle = new String(chunkHandle).toString();
						int offset = getPayloadInt(ois);
						int numberOfBytes = getPayloadInt(ois);
						// response 
						byte[] result = cs.getChunk(handle, offset, numberOfBytes);
						// write payload size
						// write payload 
						if (result == null) {
							oos.writeInt(4);
						} else {
							oos.writeInt(4 + result.length);
							oos.write(result);
						}
						oos.flush();
					} else {
						System.out.println("received unparsable command: " + command);
					}
				}
			} catch (IOException ioe) {
				System.out.println("iow in chunkserver accepting client connection " + ioe.getMessage());
			}
		}
	}
	
	public static byte[] getPayload(ObjectInputStream ois, int payloadSize) {
		
		byte[] payload = new byte[payloadSize];
		byte[] temp = new byte[payloadSize];
		int totalRead = 0;
		
		while (totalRead != payloadSize) {
			int currRead = -1;
			
			try {
				// read bytes from stream into byte array and add byte by byte to 
				// final byte array 
				currRead = ois.read(temp, 0, (payloadSize - totalRead));
				for (int i = 0; i < currRead; i++) {
					payload[totalRead +   i] = temp[i];
				}
				
			} catch (IOException ioe) {
				System.out.println("ioe in reading payload s" + ioe.getMessage());
				try {
					s.close();
					System.out.println("closed client socket connection");
				} catch (IOException e) {
					System.out.println("iow in closing client socket connection " + e.getMessage());
				}
				return null; 
			}
			if (currRead == -1) {
				System.out.println("error in reading payload");
				return null; 
			} else {
				totalRead += currRead; 
			}
		}
		
		return payload; 
	}
	
	public static int getPayloadInt(ObjectInputStream ois) {
		// read payload size from stream and return as int; return -2 if error 
		int payloadSize = -1; 
		byte[] payload = getPayload(ois, 4); // 4 is size of int 
		if (payload != null) {
			payloadSize = ByteBuffer.wrap(payload).getInt();
		}
		return payloadSize; 
	}
	
	public static void main(String [] args)
	{
		startChunkServer();
	}

}
