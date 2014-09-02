//    License: Microsoft Public License (Ms-PL) 
package org.apache.lucene.stor.azure;

import java.util.HashMap;

import com.sun.corba.se.impl.orbutil.concurrent.Mutex;

public class MutexFactory {
	private static HashMap<String, Mutex> mutexMap = new HashMap<String, Mutex>();
	
	public static Mutex getMutex(String name) {
		synchronized (MutexFactory.class) {
			if (mutexMap.get(name) != null)
				return mutexMap.get(name);
			
			Mutex mutex = new Mutex();
			mutexMap.put(name, mutex);
			return mutexMap.get(name);
		}
	}
}
