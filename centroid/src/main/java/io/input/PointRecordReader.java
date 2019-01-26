package io.input;

import io.writable.impl.PointWritable;

public class PointRecordReader extends ClusterRecordReader<PointWritable>{

	@Override
	protected void setWritableClass() {
		this.writableClass = PointWritable.class;
	}
	
}
