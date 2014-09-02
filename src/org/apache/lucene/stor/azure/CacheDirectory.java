//    License: Microsoft Public License (Ms-PL) 
package org.apache.lucene.stor.azure;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.lucene.store.SimpleFSDirectory;

public class CacheDirectory extends SimpleFSDirectory {

	public CacheDirectory(File path) throws IOException {
		super(path);
		// TODO Auto-generated constructor stub
	}
	
	public long fileLastModified(String name) throws FileNotFoundException {
		ensureOpen();
	    File file = new File(directory, name);
	    final long lastModified = file.lastModified();
	    
	    if (lastModified == 0 && !file.exists()) {
	        throw new FileNotFoundException(name);
	      } else {
	        return lastModified;
	      }
	}
	
	public void setFileLastModified(String name, long lastModified) {
		ensureOpen();
		File file = new File(directory, name);
		if (file.exists()) {
			file.setLastModified(lastModified);
		}
	}
	
	public FileOutputStream createCachedOutputStream(String name) throws IOException {
		ensureOpen();
		File file = new File(directory, name);
		if (!file.exists())
			file.createNewFile();
		FileOutputStream outputStream = new FileOutputStream(file);
		return outputStream;
	}
	
	public FileInputStream openInputStream(String name) throws FileNotFoundException {
		ensureOpen();
		File file = new File(directory, name);
		FileInputStream inputStream = new FileInputStream(file);
		return inputStream;
	}
}
