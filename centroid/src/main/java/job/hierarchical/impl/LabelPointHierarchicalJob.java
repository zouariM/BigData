package job.hierarchical.impl;

import org.apache.hadoop.mapreduce.Job;

import io.writable.impl.LabelPointWritable;
import job.hierarchical.ClusterHierarchicalJob;
import job.hierarchical.ClusterHierarchicalMapper;
import job.hierarchical.ClusterHierarchicalPartitionner;
import job.hierarchical.ClusterHierarchicalReducer;

public class LabelPointHierarchicalJob extends ClusterHierarchicalJob{
	
	public static class Mapper extends ClusterHierarchicalMapper<LabelPointWritable>{}
	public static class Partitionner extends ClusterHierarchicalPartitionner<LabelPointWritable>{}
	public static class Reducer extends ClusterHierarchicalReducer<LabelPointWritable>{}
	
	@Override
	protected void setMapperClass(Job job) {
		job.setMapperClass(Mapper.class);
	}

	@Override
	protected void setPartitionnerClass(Job job) {
		job.setPartitionerClass(Partitionner.class);
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
