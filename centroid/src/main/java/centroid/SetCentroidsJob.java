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

import writable.AvgWritable;

public class SetCentroidsJob extends Configured implements Tool{
	
	public static final String CLUSTER_NBR_KEY = "clusterNbr";
	public static final String OLD_CENTROIDS_PATH_KEY = "oldCentroidsPath";
	public static final String NEW_CENTROIDS_PATH_KEY = "newCentroidsPath";
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 5) {
			System.err.println("Invalid arguments: <inputPath> <outputPath> <oldCentroidsPath> <newCentroidsPath> <clusterNumber>");
			return -1;
		}
		
		Path pathIn = new Path(args[0]);
		Path pathOut = new Path(args[1]);
		Path newCentroidsPath = new Path(args[3]);
		int clusterNb = Integer.parseInt(args[4]);
		
		FileSystem fs = FileSystem.get(getConf());
		if(!fs.exists(pathIn)) {
			System.err.println(String.format("%s does not exist", pathIn));
			return -1;
		}
		
		if(fs.exists(pathOut))
			fs.delete(pathOut, true);
		
		if(fs.exists(newCentroidsPath))
			fs.delete(newCentroidsPath, false);
		fs.create(newCentroidsPath);
		
		fs.close();
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		
		conf.setInt(CLUSTER_NBR_KEY, clusterNb);
		conf.set(OLD_CENTROIDS_PATH_KEY, args[2]);
		conf.set(NEW_CENTROIDS_PATH_KEY, args[3]);
		
		FileInputFormat.addInputPath(job, pathIn);
		FileOutputFormat.setOutputPath(job, pathOut);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapperClass(CentroidMapper.class);
		job.setCombinerClass(CentroidCombiner.class);
		job.setPartitionerClass(CentroidPartionner.class);
		
		job.setReducerClass(CentroidReducer.class);
		job.setNumReduceTasks(clusterNb);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(AvgWritable.class);
		job.setOutputValueClass(NullWritable.class);
				
		return job.waitForCompletion(true)? 0:1;
	}

}
