package com.chunkserver;

import com.interfaces.ChunkServerInterface;
import java.io.File;
import java.io.IOException;

/**
 * implementation of interfaces at the chunkserver side
 * 
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "C:\\Users\\shahram\\Documents\\TinyFS-2\\csci485Disk\\"; // or C:\\newfile.txt
	public static long counter;

	/**
	 * Initialize the chunk server
	 */
	public ChunkServer() {
		// Add comment to see if github is working 
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
		
		
		String fileSeparator = System.getProperty("file.separator");
		//absolute file name with path
        String absoluteFilePath = fileSeparator+"Users"+fileSeparator+"Nataliercraun"+fileSeparator+"Documents"+fileSeparator+"TinyFS-2"+fileSeparator+"db"+fileSeparator+"testfile_" +counter;
        File file = new File(absoluteFilePath);
        try {
			if(file.createNewFile()){
			    System.out.println(absoluteFilePath+" File Created");
			} else System.out.println("File "+absoluteFilePath+" already exists");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		return null;
	}

	/**
	 * Write the byte array to the chunk at the specified offset The byte array size
	 * should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		System.out.println("writeChunk invoked:  Part 1 of TinyFS must implement the body of this method.");
		System.out.println("Returns false for now.\n");
		return false;
	}

	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		System.out.println("readChunk invoked:  Part 1 of TinyFS must implement the body of this method.");
		System.out.println("Returns null for now.\n");
		return null;
	}

}
