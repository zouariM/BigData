package centroid;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import writable.AvgWritable;

public class CentroidPartionner extends Partitioner<NullWritable, AvgWritable> {

	@Override
	public int getPartition(NullWritable key, AvgWritable value, int numPartitions) {
		return value.getK().get();
	}

}
