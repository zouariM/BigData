package job.parser.impl;

import org.apache.hadoop.mapreduce.Job;

import io.input.PointInputFormat;
import io.writable.impl.PointWritable;
import job.parser.ClusterParserJob;
import job.parser.ClusterParserMapper;
import job.parser.ClusterParserReducer;

public class PointParserJob extends ClusterParserJob{
	
	public static class Mapper extends ClusterParserMapper<PointWritable>{};
	public static class Reducer extends ClusterParserReducer<PointWritable>{};

	@Override
	protected void setInputFormatClass(Job job) {
		job.setInputFormatClass(PointInputFormat.class);
	}

	@Override
	protected void setMapperClass(Job job) {
		job.setMapperClass(Mapper.class);	
	}

	@Override
	protected void setCombinerClass(Job job) {
		job.setCombinerClass(Reducer.class);
	}

	@Override
	protected void setReducerClass(Job job) {
		job.setReducerClass(Reducer.class);
	}

	@Override
	protected void setOutputKeyClass(Job job) {
		job.setOutputKeyClass(PointWritable.class);
	}

}
