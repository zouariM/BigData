package output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import writable.PointWritable;

public class PointOutputFormat extends FileOutputFormat<PointWritable, NullWritable>{
	
	@Override
	public RecordWriter<PointWritable, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path path = getDefaultWorkFile(job, "");
		DataOutputStream out = new DataOutputStream(fs.create(path));
		
		return new DataRecordWriter<PointWritable, NullWritable>(out);
	}


}
