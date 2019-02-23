package job.centroid.impl;

import org.apache.hadoop.mapreduce.Job;

import io.writable.impl.PointWritable;
import job.centroid.ClusterCentroidJob;
import job.centroid.ClusterCentroidMapper;

public class PointCentroidJob extends ClusterCentroidJob{
	
	public static class Mapper extends ClusterCentroidMapper<PointWritable>{}
	
	@Override
	protected void setMapperClass(Job job) {
		job.setMapperClass(Mapper.class);
	}

}
