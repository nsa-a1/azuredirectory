//    License: Microsoft Public License (Ms-PL) 
package org.apache.lucene.stor.azure;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.sun.corba.se.impl.orbutil.concurrent.Mutex;

public class AzureIndexInput extends IndexInput {
	
	private AzureDirectory azureDirectory;
    private CloudBlobContainer blobContainer;
    private CloudBlockBlob blob;
    private CacheDirectory cacheDirectory;
    private String name;

    private IndexInput indexInput;
    private Mutex fileMutex;
    
    private AzureIndexInput parent;
    
    private ArrayList<AzureIndexInput> clones = null;
    private boolean isClone = false;

	protected AzureIndexInput(String resourceDescription) {
		super(resourceDescription);
	}
	
	public AzureIndexInput(AzureDirectory azureDirectory, CloudBlockBlob blob, IOContext context) 
			throws URISyntaxException, InterruptedException, IOException, StorageException {
		super(blob.getName());
		name = blob.getName();
		this.blob = blob;
		this.azureDirectory = azureDirectory;
		blobContainer = azureDirectory.getBlobContainer();
		cacheDirectory = azureDirectory.getCacheDirectory();
		clones = new ArrayList<AzureIndexInput>();
		fileMutex = MutexFactory.getMutex(name);
		parent = null;
		try {
			fileMutex.acquire();
			
			try {
				boolean fFileNeeded = false;
				if (!cacheDirectory.fileExists(name)) {
					fFileNeeded = true;
				}
				else {
					blob.downloadAttributes();
					long blobLength = blob.getProperties().getLength();
					long cachedLength = cacheDirectory.fileLength(name);
					long blobLastModified = blob.getProperties().getLastModified().getTime();
										
					if (blobLength != cachedLength) {
						fFileNeeded = true;
					} else {
						long cachedLastModified = cacheDirectory.fileLastModified(name);
						
						if (blobLastModified != cachedLastModified) {
							fFileNeeded = true;
						}
					}
				}
				
				if (fFileNeeded) {
					FileOutputStream stream = cacheDirectory.createCachedOutputStream(name);
					blob.download(stream);
					stream.flush();
					stream.close();
					cacheDirectory.setFileLastModified(name, blob.getProperties().getLastModified().getTime());
				}
				indexInput = cacheDirectory.openInput(name, context);
			} finally {
				fileMutex.release();
			}
		} catch (InterruptedException e) {
			throw new InterruptedException(e.getMessage());
		}
		
	}
	
	public AzureIndexInput(AzureIndexInput cloneInput) throws InterruptedException {
		super(cloneInput.name);
		fileMutex = MutexFactory.getMutex(cloneInput.name);
		try {
			fileMutex.acquire();
			
			try {
				azureDirectory = cloneInput.azureDirectory;
				blobContainer = cloneInput.blobContainer;
				indexInput = cloneInput.indexInput.clone();
				name = cloneInput.name;
				blob = cloneInput.blob;
				cacheDirectory = cloneInput.cacheDirectory;
				isClone = true;
				parent = cloneInput;
				AzureIndexInput tmp = cloneInput;
				while(tmp.isClone) {
					tmp = tmp.parent;
				}
				tmp.clones.add(this);
			} finally {
				fileMutex.release();
			}
			
		} catch (InterruptedException e) {
			throw new InterruptedException(e.getMessage());
		}
	}
	
	private AzureIndexInput(AzureIndexInput input, IndexInput slicedIndex, String sliceName) throws InterruptedException {
		super(sliceName);
		fileMutex = MutexFactory.getMutex(input.name);
		fileMutex.acquire();
		try {
			azureDirectory = input.azureDirectory;
			blobContainer = input.blobContainer;
			indexInput = slicedIndex;
			name = input.name;
			cacheDirectory = input.cacheDirectory;
			blob = input.blob;
			parent = input;
			isClone = true;
			AzureIndexInput tmp = input;
			while(tmp.isClone) {
				tmp = tmp.parent;
			}
			tmp.clones.add(this);
		} finally {
			fileMutex.release();
		}
	}

	@Override
	public void close() throws IOException {
		if (!isClone) {
			for (AzureIndexInput i : clones) {
				try {
					i.close();
				} catch(IOException e) {
					//Should never happen.
					//Lucene never closes cloned IndexInput
				}
			}
		}
		indexInput.close();
	}

	@Override
	public long getFilePointer() {
		return indexInput.getFilePointer();
	}

	@Override
	public long length() {
		return indexInput.length();
	}

	@Override
	public void seek(long arg0) throws IOException {
		indexInput.seek(arg0);
	}

	@Override
	public IndexInput slice(String arg0, long arg1, long arg2)
			throws IOException {
		IndexInput slicedInput = indexInput.slice(arg0, arg1, arg2);
		try {
			AzureIndexInput input = new AzureIndexInput(this, slicedInput, arg0);
			return (IndexInput) input;
		} catch (InterruptedException e) {
			throw new IOException("Unable to acquire mutex.", e);
		}
	}

	@Override
	public byte readByte() throws IOException {
		return indexInput.readByte();
	}

	@Override
	public void readBytes(byte[] arg0, int arg1, int arg2) throws IOException {
		indexInput.readBytes(arg0, arg1, arg2);
	}
	
	@Override
	public IndexInput clone() {
		IndexInput clone = null;
		try {
		synchronized(this) {
			AzureIndexInput input = new AzureIndexInput(this);
			clone = (IndexInput) input;
			return clone;
		}
		} catch (InterruptedException e) {
			return null;
		}
	}

}
