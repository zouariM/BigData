package hierarchical;

import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import writable.PointWritable;

public class HierarchicalJob extends Configured implements Tool{

	public static final String CENTROIDS_PATH = "centroidsPath";
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 5) 
			throw new IllegalArgumentException("Invalid arguments, try <inputPath> <ouputPath> "
											+ "<centroidsPath> <clusterNb> <levelsNb>");

		Path pathIn = new Path(args[0]);
		Path pathOut = new Path(args[1]);
		int clusterNb = Integer.parseInt(args[3]);
		int levelsNb = Integer.parseInt(args[4]);
		
		FileSystem fs = FileSystem.get(getConf());
		
		if(!fs.exists(pathIn))
			throw new FileNotFoundException(String.format("%s ** input path does not exist", pathIn));
		
		if(fs.exists(pathOut))
			fs.delete(pathOut, true);
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName(this.getClass().getSimpleName());
		
		Configuration conf = job.getConfiguration();
		conf.set(CENTROIDS_PATH, args[2]);
		
		SequenceFileInputFormat.addInputPath(job, pathIn);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, pathOut);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapperClass(HierarchicalMapper.class);
		job.setPartitionerClass(HierarchicalPartitionner.class);
		job.setReducerClass(HierarchicalReducer.class);
		int numReducers = (int)Math.pow(clusterNb, levelsNb);
		job.setNumReduceTasks(numReducers);
		
		job.setOutputKeyClass(PointWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

}
