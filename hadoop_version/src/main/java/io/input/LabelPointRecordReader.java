package io.input;

import io.writable.impl.LabelPointWritable;

public class LabelPointRecordReader extends ClusterRecordReader<LabelPointWritable>{

	@Override
	protected void setWritableClass() {
		this.writableClass = LabelPointWritable.class;
	}

}
