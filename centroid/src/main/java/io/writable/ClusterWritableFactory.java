package io.writable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import io.writable.impl.LabelPointWritable;
import io.writable.impl.PointWritable;

public class ClusterWritableFactory {
	
	private static final Map<Class<? extends ClusterWritable>, Supplier<? extends ClusterWritable>> factoryMap;
	
	static {
		factoryMap = new HashMap<>();
		
		factoryMap.put(PointWritable.class, ()->new PointWritable());
		factoryMap.put(LabelPointWritable.class, ()->new LabelPointWritable());
	}
	
	public static ClusterWritable getInstance(Class<? extends ClusterWritable> c) {
		return factoryMap.get(c).get();
	}
}
