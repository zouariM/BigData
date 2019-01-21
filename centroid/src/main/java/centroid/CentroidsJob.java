package centroid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import output.PointOutputFormat;
import writable.AvgWritable;
import writable.PointWritable;


public class CentroidsJob extends Configured implements Tool{
	
	public static final String CENTROIDS_NB_KEY = "centroidsNb";
	public static final String OLD_CENTROIDS_PATH_KEY = "oldCentroidsPath";
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 5) {
			System.err.println("Invalid arguments: <inputPath> <outputPath> <centroidsPath> "
								+ "<clusterNumber> <levelsNumber>");
			return -1;
		}
		
		Path inputPath = new Path(args[0]);
		Path pathOut = new Path(args[1]);
		int clusterNb = Integer.parseInt(args[3]);
		int levels = Integer.parseInt(args[4]);
		
		
		FileSystem fs = FileSystem.get(getConf());
		
		FileStatus seq[] = fs.listStatus(inputPath, p -> p.getName().startsWith("part"));

		if(fs.exists(pathOut))
			fs.delete(pathOut, true);
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		Configuration conf = job.getConfiguration();
		job.setJobName(this.getClass().getSimpleName());
		
		int numReducers = (int)Math.pow(clusterNb, levels);
		conf.setInt(CENTROIDS_NB_KEY, numReducers);
		conf.set(OLD_CENTROIDS_PATH_KEY, args[2]);
		
		for(FileStatus seqFile:seq)
			SequenceFileInputFormat.addInputPath(job, seqFile.getPath());
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileOutputFormat.setOutputPath(job, pathOut);
		job.setOutputFormatClass(PointOutputFormat.class);

		job.setMapperClass(CentroidMapper.class);
		job.setCombinerClass(CentroidCombiner.class);
		job.setPartitionerClass(CentroidPartionner.class);
		
		job.setReducerClass(CentroidReducer.class);
		job.setNumReduceTasks(numReducers);
		
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(AvgWritable.class);
		job.setOutputValueClass(PointWritable.class);
				
		return job.waitForCompletion(true)? 0:1;
	}

}
