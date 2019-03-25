package com.chunkserver;

import com.interfaces.ChunkServerInterface;
import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "/Users/Nataliercraun/Documents/TinyFS-2/db/"; 
	public static long counter;

	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		// Need to create metadata file here 
		System.out.println(
				"Constructor of ChunkServer is invoked:  Part 1 of TinyFS must implement the body of this method.");
		System.out.println("It does nothing for now.\n");
	}

	/**
	 * Each chunk corresponds to a file. Return the chunk handle of the last chunk
	 * in the file.
	 */
	public String initializeChunk() {
		System.out.println("createChunk invoked:  Part 1 of TinyFS must implement the body of this method.");
		System.out.println("Returns null for now.\n");
		
		String filename = "testfile_" + counter; 
        String absoluteFilePath = filePath+filename;
        File file = new File(absoluteFilePath);
        try {
			if(file.createNewFile()){
			    System.out.println(absoluteFilePath+" File Created " + " and file path is " + absoluteFilePath);
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
		System.out.println("writeChunk invoked:  Part 1 of TinyFS must implement the body of this method.");
		System.out.println("Returns false for now.\n " + "Payload: " + payload);
		

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
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// What should I return here? 
		
		return false;
	}

	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		System.out.println("readChunk invoked:  Part 1 of TinyFS must implement the body of this method.");
		System.out.println("Returns null for now.\n");
		
		String fullname = filePath + ChunkHandle; 
		byte[] payload = new byte[NumberOfBytes];
		
		RandomAccessFile raf = null; 
		try {
			raf = new RandomAccessFile(fullname, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    try {
			raf.seek(offset);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    try {
			raf.read(payload);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		
		return payload;
	}

}
