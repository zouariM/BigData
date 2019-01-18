package centroid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import output.PointOutputFormat;
import writable.AvgWritable;
import writable.PointWritable;

public class SetCentroidsJob extends Configured implements Tool{
	
	public static final String CLUSTER_NBR_KEY = "clusterNbr";
	public static final String OLD_CENTROIDS_PATH_KEY = "oldCentroidsPath";
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 4) {
			System.err.println("Invalid arguments: <inputPath> <outputPath> <oldCentroidsPath> <clusterNumber>");
			return -1;
		}
		
		Path pathIn = new Path(args[0]);
		Path pathOut = new Path(args[1]);
		int clusterNb = Integer.parseInt(args[3]);
		
		FileSystem fs = FileSystem.get(getConf());
		if(!fs.exists(pathIn)) {
			System.err.println(String.format("%s does not exist", pathIn));
			return -1;
		}
		
		if(fs.exists(pathOut))
			fs.delete(pathOut, true);
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		
		conf.setInt(CLUSTER_NBR_KEY, clusterNb);
		conf.set(OLD_CENTROIDS_PATH_KEY, args[2]);
		
		FileInputFormat.addInputPath(job, pathIn);
		FileOutputFormat.setOutputPath(job, pathOut);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(PointOutputFormat.class);
		
		job.setMapperClass(CentroidMapper.class);
		job.setCombinerClass(CentroidCombiner.class);
		job.setPartitionerClass(CentroidPartionner.class);
		
		job.setReducerClass(CentroidReducer.class);
		job.setNumReduceTasks(clusterNb);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(AvgWritable.class);
		job.setOutputValueClass(PointWritable.class);
				
		return job.waitForCompletion(true)? 0:1;
	}

}
