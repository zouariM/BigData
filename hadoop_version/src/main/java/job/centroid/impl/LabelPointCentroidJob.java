package job.centroid.impl;

import org.apache.hadoop.mapreduce.Job;

import io.writable.impl.LabelPointWritable;
import job.centroid.ClusterCentroidJob;
import job.centroid.ClusterCentroidMapper;

public class LabelPointCentroidJob extends ClusterCentroidJob{
	
	public static class Mapper extends ClusterCentroidMapper<LabelPointWritable>{}
	
	@Override
	protected void setMapperClass(Job job) {
		job.setMapperClass(Mapper.class);
	}

}
