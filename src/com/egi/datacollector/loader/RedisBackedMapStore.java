package com.egi.datacollector.loader;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.egi.datacollector.loader.keyval.RedisClient;
import com.hazelcast.core.MapStore;

public class RedisBackedMapStore implements MapStore<String, Object> {

	@Override
	public Object load(String arg0) {
		return RedisClient.get(arg0);
	}

	@Override
	public Map<String, Object> loadAll(Collection<String> arg0) {
		
		return RedisClient.loadAll(arg0);
	}

	@Override
	public Set<String> loadAllKeys() {
		return RedisClient.loadAllKeys();
	}

	@Override
	public void delete(String arg0) {
		RedisClient.delete(arg0);

	}

	@Override
	public void deleteAll(Collection<String> arg0) {
		RedisClient.deleteAll(arg0);

	}

	@Override
	public void store(String arg0, Object arg1) {
		RedisClient.set(arg0, (Serializable) arg1);

	}

	@Override
	public void storeAll(Map<String, Object> arg0) {
		RedisClient.storeAll(arg0);

	}

}
