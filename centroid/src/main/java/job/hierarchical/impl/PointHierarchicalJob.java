package job.hierarchical.impl;

import org.apache.hadoop.mapreduce.Job;

import io.writable.impl.PointWritable;
import job.hierarchical.ClusterHierarchicalJob;
import job.hierarchical.ClusterHierarchicalMapper;
import job.hierarchical.ClusterHierarchicalPartitionner;
import job.hierarchical.ClusterHierarchicalReducer;

public class PointHierarchicalJob extends ClusterHierarchicalJob{
	
	public static class Mapper extends ClusterHierarchicalMapper<PointWritable>{}
	public static class Partitionner extends ClusterHierarchicalPartitionner<PointWritable>{}
	public static class Reducer extends ClusterHierarchicalReducer<PointWritable>{}
	
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
		job.setOutputKeyClass(PointWritable.class);
	}
	
}
