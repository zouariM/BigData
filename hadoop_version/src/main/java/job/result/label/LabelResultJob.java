package job.result.label;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

import io.writable.impl.LabelPointWritable;

public class LabelResultJob extends Configured implements Tool{
		
	public static class Mapper extends InverseMapper<LabelPointWritable, NullWritable>{}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2)
			throw new IllegalArgumentException("Invalid arguments, try <inputPath> <outputPath>");
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileSystem fs = FileSystem.get(getConf());
		if(fs.exists(outputPath))
			fs.delete(outputPath, true);
	
		Job job = Job.getInstance();
		job.setJarByClass(getClass());
		job.setJobName(this.getClass().getSimpleName());
				
		for(FileStatus file:fs.listStatus(inputPath, p->p.getName().startsWith("part")))
			SequenceFileInputFormat.addInputPath(job, file.getPath());	
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(Mapper.class);
		job.setCombinerClass(LabelResultCombiner.class);
		job.setReducerClass(LabelResultReducer.class);
				
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(LabelPointWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0:1;
	}

}
