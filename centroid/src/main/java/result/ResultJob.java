package result;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class ResultJob extends Configured implements Tool{
	
	public static final String COLUMNS_KEY = "columns";
	public static final String RESULT_PATH_KEY = "result";
	public static final String CENTROIDS_PATH = "centroidsPath";
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 5) {
			System.err.println("Invalid arguments: try <inputPath> <resultPath> <outputPath> <columns> <centroidsPath>");
			return -1;
		}
		
		FileSystem fs = FileSystem.get(getConf());
		Path input = new Path(args[0]);
		Path resultPath = new Path(args[1]);
		Path output = new Path(args[2]);
		
		if(!fs.exists(input))
			throw new IllegalArgumentException(String.format("%s not found", input));
		
		if(fs.exists(resultPath))
			fs.delete(resultPath, false);
		fs.create(resultPath);
		
		if(fs.exists(output))
			fs.delete(output, true);
		
		fs.close();
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		
		Configuration conf = job.getConfiguration();
		conf.set(RESULT_PATH_KEY, args[1]);
		conf.set(COLUMNS_KEY, args[3]);
		conf.set(CENTROIDS_PATH, args[4]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setMapperClass(ResultMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

}
