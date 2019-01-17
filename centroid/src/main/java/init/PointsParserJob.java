package init;

import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import input.PointInputFormat;
import writable.PointWritable;

public class PointsParserJob extends Configured implements Tool{
	
	public static final String PARSER_JOB_DATA_COUNTER ="Parser Job Counters";
	public static final String PARSER_JOB_INVALID_LINES_COUNTER = "Invalid lines";
	public static final String COLUMNS_ARG_KEY = "columns";
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		
		if(args.length != 3)
			throw new IllegalArgumentException("<inputPath> <outputPath> <columns>");
		
		Path pathIn = new Path(args[0]);
		Path pathOut = new Path(args[1]);
		FileSystem fs = FileSystem.get(getConf());
		
		if(!fs.exists(pathIn))
			throw new FileNotFoundException(String.format("%s not found", pathIn));
		if(fs.exists(pathOut))
			fs.delete(pathOut, true);		

		Configuration conf = job.getConfiguration();
		conf.set(COLUMNS_ARG_KEY, args[2]);
		
		FileInputFormat.addInputPath(job, pathIn);
		job.setInputFormatClass(PointInputFormat.class);
		FileOutputFormat.setOutputPath(job, pathOut);
		
		job.setMapperClass(PointsParserMapper.class);
		job.setCombinerClass(PointsParserReducer.class);
		job.setReducerClass(PointsParserReducer.class);
		
		job.setOutputKeyClass(PointWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

}
