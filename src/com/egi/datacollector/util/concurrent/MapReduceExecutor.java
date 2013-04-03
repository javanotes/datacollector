package com.egi.datacollector.util.concurrent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.RecursiveTask;

import com.egi.datacollector.processor.mapreduce.IMapper;
import com.egi.datacollector.processor.mapreduce.IReducer;
import com.egi.datacollector.processor.mapreduce.KeyValue;
import com.egi.datacollector.util.Config;

public class MapReduceExecutor<X extends Serializable, K extends Serializable, V extends Serializable> extends RecursiveAction{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2219296141359196377L;
	private final IMapper<X, K, V> mapper;
	private final IReducer<K, V> reducer;
	
	private final ForkJoinPool executor;

	public MapReduceExecutor(IMapper<X, K, V> map, IReducer<K, V> reduce){
		this.mapper = map;
		this.reducer = reduce;
		executor = new ForkJoinPool(Config.getForkJoinParallelism());
		executor.invoke(this);
		
		System.err.println("*************** EXPERIMENTAL. USAGE IS DISCOURAGED AND UNWARRANTED. *******************");
	}
	
	/**
	 * 
	 * @param eachData
	 */
	public void execute(X eachData){
		if(eachData != null){
			
			MapTask task = new MapTask(eachData);
			//executor.invoke(task)
			task.fork();		
		}
		
	}
	
	private void aggregate( Collection<KeyValue<K, V>> items){
		
		Map<K, Collection<V>> combine = new HashMap<K, Collection<V>>();
		for(KeyValue<K, V> keyVal : items){
			//sort by key and combine the values
			if(combine.containsKey(keyVal.key)){
				combine.get(keyVal.key).add(keyVal.value);
			}
			else{
				combine.put(keyVal.key, new ArrayList<V>());
				combine.get(keyVal.key).add(keyVal.value);
			}
			
		}
		
		for(Entry<K, Collection<V>> entry : combine.entrySet()){
			
			ReduceTask task = new ReduceTask(entry.getKey(), entry.getValue());
			//now reduce the aggregations for each key
			//reduceRouter.tell(new ReduceMsg<K, V>(entry.getValue(), entry.getKey()), getSelf());
			//counter++;			
		}
		
			
	}
	
	private class MapTask extends RecursiveTask<Collection<KeyValue<K, V>>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 4331312566772412710L;
		final X entry;
		
		MapTask(X element){
			entry = element;
		}

		@Override
		protected Collection<KeyValue<K, V>> compute() {
			return mapper.mapToKeyValue(entry);
						
		}
		
	}
	
	private class ReduceTask extends RecursiveTask<KeyValue<K, V>>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 3341979138303699934L;

		final K key;
		final Collection<V> values;
		
		ReduceTask(K k, Collection<V> v){
			key = k;
			values = v;
		}
		
		@Override
		protected KeyValue<K, V> compute() {
			return reducer.reduceForKey(key, values);
			
		}
		
	}
		
	@Override
	protected void compute() {
		// TODO Auto-generated method stub
		
	}

}
