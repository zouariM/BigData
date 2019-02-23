package io.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import io.writable.ClusterWritable;
import io.writable.ClusterWritableFactory;

public abstract class ClusterRecordReader<T extends ClusterWritable<T>> extends RecordReader<LongWritable, T>{
	
	private LineRecordReader lineRecordReader;
	private LongWritable key;
	private T value;
	
	private Configuration conf;
	private int[] columns;
	protected Class<? extends ClusterWritable<T>> writableClass;
	
	public ClusterRecordReader() {
		super();
		this.setWritableClass();
	}
	
	protected abstract void setWritableClass();
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		lineRecordReader = new LineRecordReader();
		lineRecordReader.initialize(split, context);
		
		conf = context.getConfiguration();
		columns = conf.getInts("columns");	
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!lineRecordReader.nextKeyValue()) {
			return false;
		}
		
		key = lineRecordReader.getCurrentKey();
		String currentLine = lineRecordReader.getCurrentValue().toString();

		try {
			value = (T)ClusterWritableFactory.getInstance(writableClass);
			value.parseLine(currentLine, columns);
		}
		catch(IllegalArgumentException ex) {
			System.err.println(ex.getMessage());
			value = null;
		}		
		
		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public T getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		lineRecordReader.close();
	}

}
