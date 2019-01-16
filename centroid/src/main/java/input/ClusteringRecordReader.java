package input;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import writable.PointWritable;

public class ClusteringRecordReader extends RecordReader<PointWritable, NullWritable>{
	
	private LineRecordReader lineRecordReader;	
	private PointWritable key;
	private HashSet<PointWritable> keys;
	
	private Configuration conf;
	private int[] columns;
		
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		lineRecordReader = new LineRecordReader();
		lineRecordReader.initialize(split, context);
		
		conf = context.getConfiguration();
		columns = conf.getInts("columns");	
		keys = new HashSet<>();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!lineRecordReader.nextKeyValue()) {
			return false;
		}
			
		String currentLine = lineRecordReader.getCurrentValue().toString();

		try {
			PointWritable vector = new PointWritable(currentLine, columns);
			if(keys.add(vector)) {
				key = vector;
				return true;
			}
			else
				return nextKeyValue();
		}
		catch(IllegalArgumentException ex) {
			return nextKeyValue();
		}		
	}

	@Override
	public PointWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public NullWritable getCurrentValue() throws IOException, InterruptedException {
		return NullWritable.get();
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
