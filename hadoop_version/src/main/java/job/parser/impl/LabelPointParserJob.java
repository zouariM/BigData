package job.parser.impl;

import org.apache.hadoop.mapreduce.Job;

import io.input.LabelPointInputFormat;
import io.writable.impl.LabelPointWritable;
import job.parser.ClusterParserJob;
import job.parser.ClusterParserMapper;
import job.parser.ClusterParserReducer;

public class LabelPointParserJob extends ClusterParserJob{
	
	public static class Mapper extends ClusterParserMapper<LabelPointWritable>{}
	public static class Reducer extends ClusterParserReducer<LabelPointWritable>{}
	
	@Override
	protected void setInputFormatClass(Job job) {
		job.setInputFormatClass(LabelPointInputFormat.class);
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
		job.setOutputKeyClass(LabelPointWritable.class);
	}

}
