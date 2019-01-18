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

public class PointOutputFormat extends FileOutputFormat<NullWritable, PointWritable>{

	@Override
	public RecordWriter<NullWritable, PointWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		setOutputName(job, "centroids" + job.getNumReduceTasks());
		Path path = getDefaultWorkFile(job, ".ser");
		DataOutputStream out = new DataOutputStream(fs.create(path));
		
		return new PointRecordWriter(out);
	}

}
