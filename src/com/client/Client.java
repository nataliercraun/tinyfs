package com.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * 
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	final static String filePath = "/Users/Nataliercraun/Documents/TinyFS-2/db/"; 
	public static ChunkServer cs = new ChunkServer();
	
	// Should all of these be here??
	
	public static ObjectOutputStream oos;
	public static ObjectInputStream ois; 
	public static ServerSocket ss; 
	public static int  port = -1; 
	public static InetAddress address;
	public static Socket s; 
	

	/**
	 * Initialize the client
	 */
	public Client() {
		if (cs == null)
			cs = new ChunkServer();
		
		String filename = "portnum"; 
        String absoluteFilePath = filePath+filename;
        File file = new File(absoluteFilePath);
		
		try {
			
			 BufferedReader br = null;
				try {
					br = new BufferedReader(new FileReader(file));
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				String temp = br.readLine();
				if (temp == null) {
					port = 0; 
				} else {
					port = Integer.valueOf(temp);
					System.out.println("Client attempting to connect to port: " + port);
				}
			
			// Is this the correct hostname
			if (s == null) {
				s = new Socket("localhost", port);
				oos = new ObjectOutputStream(s.getOutputStream());
				ois = new ObjectInputStream(s.getInputStream());
			} 
			
			
			System.out.println("Connected");
			
			
		} catch (IOException ioe) {
			System.out.println("Problem connecting client");
		}
	}
	

	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		try {
			// request 
			// write payload size (2 ints, 4 + 4)
			oos.writeInt(8);
			// write command identifier 
			oos.writeInt(ChunkServer.InitializeChunk);
			oos.flush();
			// parse response 
			int chunkHandleSize = getPayloadInt(ois);
			// reduce size by payload size int 
			chunkHandleSize -= 4; 
			byte[] handle = getPayload(ois, chunkHandleSize);
			return (new String(handle).toString());
			
		} catch (IOException ioe) {
			System.out.println("ioe in initialize chunk " + ioe.getMessage());
		}
		return null; 
	}

	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		
		if (offset + payload.length > ChunkServer.ChunkSize) {
			System.out.println("The chunk should write should be within range of the file, invalid");
			return false; 
		} 
		try {
			// request 
			byte[] handle = ChunkHandle.getBytes();
			// write payload size 
			oos.writeInt(4 + 4 + 4 + 4 + 4 + handle.length + payload.length);
			// write command identifier 
			oos.writeInt(ChunkServer.PutChunk);
			// write chunkHandle size 
			oos.writeInt(handle.length);
			// write chunk handle
			oos.write(handle);
			// write payload size s
			oos.writeInt(payload.length);
			// write payload 
			oos.write(payload);
			// write offset
			oos.writeInt(offset);
			oos.flush();
			// parse response 
			int resultSize = getPayloadInt(ois);
			int result = getPayloadInt(ois);
			
			if (result == 0) {
				return false; 
			} else {
				System.out.println("success put chunk");
				return true; 
			}
			
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
		}
		
		return false; 
	}

	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		
		byte[] payload = null;
		
		try {
			// request 
			byte[] handle = ChunkHandle.getBytes();
			// write payload size (4-command + 4-handle-length + actual handle)
			oos.writeInt(4 + 4 + 4 + 4 + handle.length);
			// write command identifier 
			oos.writeInt(ChunkServer.GetChunk);
			// write chunkHandle length 
			oos.writeInt(handle.length);
			// write chunk handle
			oos.write(handle);
			// write offset 
			oos.writeInt(offset);
			// write number of bytes
			oos.writeInt(NumberOfBytes);
			oos.flush();
			// parse response 
			int payloadSize = getPayloadInt(ois);
			System.out.println("Recieving this size: " + payloadSize);
			payload = getPayload(ois, payloadSize);
			System.out.print("received payload in client");
			
			return payload; 
			
		} catch (IOException ioe) {
			System.out.println(ioe.getMessage());
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
					System.out.println("ioe in reading payload" + ioe.getMessage());
					try {
						s.close();
						System.out.println("closed client socket connection");
					} catch (IOException e) {
						System.out.println("ioe in closing client socket connection " + e.getMessage());
					}
					return null; 
				}
				if (currRead == -1) {
					System.out.println("error in reading payload");
					return null; 
				} else {
					totalRead += currRead; 
				}
				System.out.println("total Read: " + totalRead);
				System.out.println("payloadsize: " + payloadSize);
			}
			return payload; 
		}

}
