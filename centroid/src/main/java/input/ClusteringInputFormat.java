package input;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import writable.PointWritable;

public class ClusteringInputFormat extends FileInputFormat<PointWritable, NullWritable>{

	@Override
	public RecordReader<PointWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new ClusteringRecordReader();
		}

}
