package job.result.hierarichical;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class ResultJob extends Configured implements Tool{
	
	public static final String COLUMNS_KEY = "columns";
	public static final String OUTPUT_PATH = "outputPath";
	public static final String CENTROIDS_PATH = "centroidsPath";
	public static final String LEVELS_NB_KEY = "levelsNb";
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 5) {
			System.err.println("Invalid arguments: try <inputPath> <outputPath> "
								+ "<columns> <centroidsPath> <levelsNb>");
			return -1;
		}
		
		FileSystem fs = FileSystem.get(getConf());
		Path input = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		if(!fs.exists(input))
			throw new IllegalArgumentException(String.format("%s not found", input));
		
		if(fs.exists(outputPath))
			fs.delete(outputPath, true);
		
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName(this.getClass().getSimpleName());

		Configuration conf = job.getConfiguration();
		conf.set(OUTPUT_PATH, args[1]);
		conf.set(COLUMNS_KEY, args[2]);
		conf.set(CENTROIDS_PATH, args[3]);
		conf.setInt(LEVELS_NB_KEY, Integer.parseInt(args[4]));
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(ResultMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

}
