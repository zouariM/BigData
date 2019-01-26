package io.writable;

import org.apache.hadoop.io.WritableComparable;

import io.writable.impl.PointWritable;

public interface ClusterWritable<T> extends WritableComparable<T>, Cloneable{
	void parseLine(String line, int columns[]);
	PointWritable getPoint();
	
	default Double distanceTo(ClusterWritable<T> point) {
		return this.getPoint().distanceTo(point.getPoint());
	}
}
