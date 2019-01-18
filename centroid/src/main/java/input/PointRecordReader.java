package input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import writable.PointWritable;

public class PointRecordReader extends RecordReader<LongWritable, PointWritable>{
	
	private LineRecordReader lineRecordReader;
	private LongWritable key;
	private PointWritable value;
	
	private Configuration conf;
	private int[] columns;
		
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		lineRecordReader = new LineRecordReader();
		lineRecordReader.initialize(split, context);
		
		conf = context.getConfiguration();
		columns = conf.getInts("columns");	
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!lineRecordReader.nextKeyValue()) {
			return false;
		}
		
		key = lineRecordReader.getCurrentKey();
		String currentLine = lineRecordReader.getCurrentValue().toString();

		try {
			PointWritable vector = new PointWritable(currentLine, columns);
			value = vector;
		}
		catch(IllegalArgumentException ex) {
			System.err.println("message =" + ex.getMessage());
			value = null;
		}		
		
		return true;
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public PointWritable getCurrentValue() throws IOException, InterruptedException {
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
