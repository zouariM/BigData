import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import writable.PointWritable;

public class Test extends Configured implements Tool{
	
	public static class TestMapper extends Mapper<PointWritable, NullWritable, PointWritable, NullWritable>{
	}

	public static class TestPartitionner extends Partitioner<PointWritable, NullWritable> {

		@Override
		public int getPartition(PointWritable key, NullWritable value, int numPartitions) {
			MapWritable levels = key.getClusters();
			Integer reducerNum = 0;
		
			for(Writable k:levels.keySet()) {
				int x = ((IntWritable)levels.get(k)).get();
				int i  = ((IntWritable)k).get();
			
				reducerNum += x * (int)Math.pow(numPartitions, i);
			}
		
			return reducerNum;
		}
	}

	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		
		FileSystem fs = FileSystem.get(getConf());		
		FileStatus seq[] = fs.listStatus(new Path("/users/tests/seqfiles2"), p -> p.getName().startsWith("part"));
		
		Path pathOut = new Path("/users/tests/test");
		if(fs.exists(pathOut))
			fs.delete(pathOut, true);
		
		for(FileStatus seqFile:seq)
			MultipleInputs.addInputPath(job, seqFile.getPath(), SequenceFileInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, pathOut);
		
		job.setMapperClass(TestMapper.class);
		job.setPartitionerClass(TestPartitionner.class);
		job.setNumReduceTasks(2);
		
		job.waitForCompletion(true);
		return 0;
	}

	
}
