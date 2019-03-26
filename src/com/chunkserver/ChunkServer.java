package com.chunkserver;

import com.interfaces.ChunkServerInterface;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.io.FileInputStream;
import java.io.FileOutputStream;

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
		
		String filename = "metadata"; 
        String absoluteFilePath = filePath+filename;
        File file = new File(absoluteFilePath);
        FileInputStream fip = null; 
        
        try {
			if(file.createNewFile()){
			   System.out.println("Creating metadata file");
			} else {
				 try {
					 fip = new FileInputStream(file);
					} catch (FileNotFoundException e1) {
						e1.printStackTrace();
					}
			    int content;
				while ((content = fip.read()) != -1) {
					counter = content;
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
        
        
        try {
			if(file.createNewFile()){
			    System.out.println("COUNTER IS " + counter);
			    // Update metadata file
			    try {
					FileOutputStream fop = new FileOutputStream(file);
					String count = String.valueOf(counter);
					try {
						fop.write(count.getBytes());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						fop.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						fop.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					counter++; 
				} catch (FileNotFoundException e1) {
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

}
