package output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import writable.PointWritable;

public class PointRecordWriter extends RecordWriter<NullWritable, PointWritable>{

	private DataOutputStream out;
	
	public PointRecordWriter(DataOutputStream out) {
		this.out = out;
	}
	
	@Override
	public void write(NullWritable key, PointWritable value) throws IOException, InterruptedException {
		value.write(out);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		this.out.flush();
		this.out.close();
	}
}
