//    License: Microsoft Public License (Ms-PL) 
package org.apache.lucene.stor.azure;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

/**
 * 
 * @author Peter Liverovsky (aka nsa_a1)
 *
 */
public class AzureDirectory extends Directory {
	
	private String catalog;
    private CloudBlobClient blobClient;
    private CloudBlobContainer blobContainer;
    private CacheDirectory cacheDirectory;
    
    public AzureDirectory(CloudStorageAccount storageAccount) throws URISyntaxException, StorageException, IOException {
        this(storageAccount, null, null);
    }
    
    /**
     * Create AzureDirectory
     * <p>Note: default local cache is to use file system in java.io.tmpdir</p>
     * @param storageAccount storage account to use
     * @param catalog name of catalog (folder in blob storage)
     * @throws URISyntaxException
     * @throws StorageException
     * @throws IOException
     */
    public AzureDirectory(
        CloudStorageAccount storageAccount,
        String catalog) throws URISyntaxException, StorageException, IOException
    {
        this(storageAccount, catalog, null);
    }

    
    /**
     * Create an AzureDirectory
     * @param storageAccount storage account to use
     * @param catalog name of catalog (folder in blob storage)
     * @param cacheDirectory local Directory object to use for local cache
     * @throws IOException 
     * @throws StorageException 
     * @throws URISyntaxException 
     */
    public AzureDirectory(
        CloudStorageAccount storageAccount,
        String catalog,
        File cacheDirectory) throws URISyntaxException, StorageException, IOException
    {
        if (storageAccount == null)
            throw new IllegalArgumentException("storageAccount can not be null.");

        if (catalog == null)
        	this.catalog = "lucene";
        else if( catalog == "")
            this.catalog = "lucene";
        else
        	this.catalog = catalog.toLowerCase();

        blobClient = storageAccount.createCloudBlobClient();
        initCacheDirectory(cacheDirectory);
    }
    
    private void initCacheDirectory(File cacheDirectory) throws URISyntaxException, StorageException, IOException
    {
        if (cacheDirectory != null)
        {
            // save it off
            this.cacheDirectory = new CacheDirectory(cacheDirectory);
        }
        else
        {
            Path cachePath = Paths.get(System.getProperty("java.io.tmpdir"), "AzureDirectory");
            File azureDir = cachePath.toFile();
            if (!azureDir.exists())
                azureDir.mkdir();

            File catalogDir = new File(Paths.get(cachePath.toString(), catalog).toUri());
            if (!catalogDir.exists())
                catalogDir.mkdir();
            
            this.cacheDirectory = new CacheDirectory(catalogDir);
        }

        CreateContainer();
    }
    
    public void CreateContainer() throws URISyntaxException, StorageException
    {
        blobContainer = blobClient.getContainerReference(catalog);

        // create it if it does not exist
        blobContainer.createIfNotExists();
    }
    
    public void ClearCache() throws IOException
    {
        for (String file : cacheDirectory.listAll())
        {
            cacheDirectory.deleteFile(file);
        }
    }

	@Override
	public void clearLock(String name) throws IOException {
		synchronized(this) {
			if (locks.containsKey(name)) {
				locks.get(name).BreakeLock();
			}
			cacheDirectory.clearLock(name);
		}

	}

	@Override
	public void close() throws IOException {
		cacheDirectory.close();
		blobContainer = null;
		blobClient = null;
	}

	@Override
	public IndexOutput createOutput(String name, IOContext context)
			throws IOException {
		try {
			CloudBlockBlob blob = blobContainer.getBlockBlobReference(name);
			AzureIndexOutput output = new AzureIndexOutput(this, blob, context);
			return output;
		} catch (URISyntaxException | StorageException | InterruptedException e) {
			throw new IOException("Unable to create output.", e);
		}
	}

	@Override
	public void deleteFile(String name) throws IOException {
		try {
			CloudBlockBlob blob = blobContainer.getBlockBlobReference(name);
			blob.deleteIfExists();
			System.out.println("DELETE " + blobContainer.getUri().toString() + 
					"/" + name);
			
			if (cacheDirectory.fileExists(name + ".blob"))
				cacheDirectory.deleteFile(name + ".blob");
			if (cacheDirectory.fileExists(name))
				cacheDirectory.deleteFile(name);
		} catch (URISyntaxException | StorageException e) {
			throw new IOException("Unable to get blob from cloud. For more details see cause.", e);
		}

	}

	/**
	 * @return Returns true if a file with the given name exists.
	 */
	@Override
	public boolean fileExists(String fileName) throws IOException {
		// this always comes from the server
		try {
			CloudBlockBlob blob = blobContainer.getBlockBlobReference(fileName);
			return blob.exists();
		} catch (URISyntaxException | StorageException e) {
			throw new IOException("Unable to get blob from cloud. For more details see cause.", e);
		}
	}

	/**
	 * @param name a file name
	 * @return Returns the length of a file in the directory.
	 */
	@Override
	public long fileLength(String name) throws IOException {
		try {
			CloudBlockBlob blob = blobContainer.getBlockBlobReference(name);
			blob.downloadAttributes();
			return blob.getProperties().getLength();
		} catch (URISyntaxException | StorageException e) {
			throw new IOException("Unable to get blob from cloud. For more details see cause.", e);
		}
	}

	@Override
	public LockFactory getLockFactory() {
		// This Directory implementation provide own locking implementation 
		return null;
	}

	/**
	 * @return Returns an array of strings, one for each file in the directory.
	 */
	@Override
	public String[] listAll() throws IOException {
		ArrayList<String> list = new ArrayList<String>();
		Iterable<ListBlobItem> results = blobContainer.listBlobs();
		
		for (ListBlobItem item : results) {
			list.add(item.getUri().toString().substring(
					item.getUri().toString().lastIndexOf("/")));
		}
		return list.toArray(new String[list.size()]);
	}

	private HashMap<String, AzureLock> locks = new HashMap<String, AzureLock>();
	
	@Override
	public Lock makeLock(String name) {
		synchronized(this) {
			if (!locks.containsKey(name)) {
				locks.put(name, new AzureLock(name, this));
			}
			return locks.get(name);
		}
	}

	/**
	 * @return returns a stream reading existing file
	 */
	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException {
		CloudBlockBlob blob;
		try {
			blob = blobContainer.getBlockBlobReference(name);
			AzureIndexInput input = new AzureIndexInput(this, blob, context);
			return input;
		} catch (URISyntaxException | StorageException | InterruptedException e) {
			throw new IOException("Unable to open input.", e);
		}
	}

	@Override
	public void setLockFactory(LockFactory arg0) throws IOException {
		UnsupportedOperationException cause = new UnsupportedOperationException("This Directory implementation provide own locking implementation");
		throw new IOException("Not implemented.", cause);
	}

	@Override
	public void sync(Collection<String> arg0) throws IOException {
		// TODO Auto-generated method stub

	}
	
	public CloudBlobContainer getBlobContainer() {
		return blobContainer;
	}
	
	public CacheDirectory getCacheDirectory() {
		return cacheDirectory;
	}
	
	public void setCacheDirectory(CacheDirectory cacheDirectory) {
		this.cacheDirectory = cacheDirectory;
	}

}
