//    License: Microsoft Public License (Ms-PL) 
package org.apache.lucene.stor.azure;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.store.Lock;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 * Implements lock semantics on AzureDirectory via a blob lease
 * @author Peter Liverovsky (aka nsa_a1)
 *
 */
public class AzureLock extends Lock {
	
	private String lockFile;
    private AzureDirectory azureDirectory;
    private String leaseid;
    
    public AzureLock(String lockFile, AzureDirectory directory) {
    	this.lockFile = lockFile;
    	azureDirectory = directory;
    }

	@Override
	public void close() throws IOException {
		if (!IsNullOrEmpty(leaseid)) {
			try {
				CloudBlockBlob blob = azureDirectory.getBlobContainer().getBlockBlobReference(lockFile);
				AccessCondition condition = new AccessCondition();
				condition.setLeaseID(leaseid);
				if (service != null) {
					service.shutdownNow();
					service.awaitTermination(30, TimeUnit.SECONDS);
					service = null;
				}
				blob.releaseLease(condition);
				leaseid = null;
			} catch (URISyntaxException | StorageException | InterruptedException e) {
				throw new IOException("A StorageException occurred. See cause.", e);
			}
		}
	}
	
	public void BreakeLock() {
		try {
			CloudBlockBlob blob = azureDirectory.getBlobContainer().getBlockBlobReference(lockFile);
			
            blob.breakLease(0);
            service.shutdown();
        } catch (Exception err) {
        }
        leaseid = null;
	}

	@Override
	public boolean isLocked() throws IOException {
		try {
			CloudBlockBlob blob = azureDirectory.getBlobContainer().getBlockBlobReference(lockFile);
			try {
				if (IsNullOrEmpty(leaseid))
				{
					String tempLease = blob.acquireLease(60, leaseid);
					if (tempLease == null || StringUtils.isEmpty(tempLease)) {
						return true;
					}
					AccessCondition condition = new AccessCondition();
					condition.setLeaseID(tempLease);
					blob.releaseLease(condition);
				}
				return IsNullOrEmpty(leaseid);
			} catch(StorageException e) {
				if( handleException(blob, e) )
					return isLocked();
			}
		} catch (StorageException | URISyntaxException e) {
			throw new IOException("A StorageException occurred. See cause.", e);
		} 
		leaseid = null;
		return false;
	}
	
	private ScheduledExecutorService service = null;

	@Override
	public boolean obtain() throws IOException {
		try {
			CloudBlockBlob blob = azureDirectory.getBlobContainer().getBlockBlobReference(lockFile);
			try {
				if (IsNullOrEmpty(leaseid))
				{
					leaseid = blob.acquireLease(60, leaseid);
					
					// keep the lease alive by renewing every 30 seconds
					service = Executors.newSingleThreadScheduledExecutor();
					service.scheduleWithFixedDelay(new Runnable() {
						@Override
						public void run() {
							try {
								Renew();
							} catch (Exception e) {
								System.err.println("Renew lock failed: " + e.getMessage());
								e.printStackTrace();
							}
						}
					}, 30, 30, TimeUnit.SECONDS);
				}
				return !IsNullOrEmpty(leaseid);
			} catch(StorageException e) {
				if( handleException(blob, e) )
					return obtain();
			}
		} catch (URISyntaxException | StorageException e) {
			throw new IOException("A StorageException occurred. See cause.", e);
		}
		return false;
	}
	
	public void Renew() throws URISyntaxException, StorageException {
		if (!IsNullOrEmpty(leaseid)) {
			CloudBlockBlob blob = azureDirectory.getBlobContainer().getBlockBlobReference(lockFile);
			AccessCondition condition = new AccessCondition();
			condition.setLeaseID(leaseid);
			blob.renewLease(condition);
		}
	}
	
	private boolean handleException(CloudBlockBlob blob, StorageException err) throws URISyntaxException, StorageException, IOException
    {
		// Create container if it does not exist
        if (err.getHttpStatusCode() == 404)
        {
            azureDirectory.CreateContainer();
            {
                blob.uploadText(lockFile);
            }
            return true;
        }
        return false;
    }
	
	private boolean IsNullOrEmpty(String str) {
		return (str == null || str.isEmpty());
	}
}
