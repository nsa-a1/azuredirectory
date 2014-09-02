//    License: Microsoft Public License (Ms-PL) 
package org.apache.lucene.stor.azure;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.sun.corba.se.impl.orbutil.concurrent.Mutex;

public class AzureIndexOutput extends IndexOutput {
	
    private String name;
    private IndexOutput indexOutput;
    private Mutex fileMutex;
    private CloudBlockBlob blob;
    private CacheDirectory cacheDirectory;
	
	public AzureIndexOutput(AzureDirectory azureDirectory, CloudBlockBlob blob, IOContext context) 
			throws URISyntaxException, InterruptedException, IOException {
		name = blob.getName();
		fileMutex = MutexFactory.getMutex(name);
		fileMutex.acquire();
		try {
			//this.azureDirectory = azureDirectory;
			this.blob = blob;
			//blobContainer = azureDirectory.getBlobContainer();
			cacheDirectory = azureDirectory.getCacheDirectory();
			
			indexOutput = cacheDirectory.createOutput(name, context);
		} finally {
			fileMutex.release();
		}
	}

	@Override
	public void close() throws IOException {
		try {
			fileMutex.acquire();
			try {
				String fileName = name;
				long length = indexOutput.getFilePointer();
				// make sure that all written out
				indexOutput.close();
				
				InputStream stream = cacheDirectory.openInputStream(fileName);
				blob.upload(stream, length);
				blob.downloadAttributes();
				cacheDirectory.setFileLastModified(fileName, blob.getProperties().getLastModified().getTime());
			} catch (StorageException e) {
				throw new IOException("", e);
			} finally {
				fileMutex.release();
			}
		} catch (InterruptedException e) {
			throw new IOException("Uneble to acquire mutex.", e);
		}
	}

	@Deprecated
	@Override
	public void flush() throws IOException {
		 indexOutput.flush();
	}

	@Override
	public long getChecksum() throws IOException {
		return indexOutput.getChecksum();
	}

	@Override
	public long getFilePointer() {
		return indexOutput.getFilePointer();
	}

	@Override
	public void writeByte(byte arg0) throws IOException {
		indexOutput.writeByte(arg0);
	}

	@Override
	public void writeBytes(byte[] arg0, int arg1, int arg2) throws IOException {
		indexOutput.writeBytes(arg0, arg1, arg2);
	}

}
