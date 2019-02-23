package job.hierarchical;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import io.writable.ClusterWritable;

public class ClusterHierarchicalPartitionner<T extends ClusterWritable<T>> extends Partitioner<T, NullWritable>{
	
	@Override
	public int getPartition(T key, NullWritable value, int numPartitions) {
		MapWritable levels = key.getPoint().getClusters();
		Integer reducerNum = 0;
		int clusterNb = (int)Math.exp(Math.log(numPartitions)/levels.size());
		
		for(Writable k:levels.keySet()) {
			int x = ((IntWritable)levels.get(k)).get();
			int i  = ((IntWritable)k).get();
			
			reducerNum += x * (int)Math.pow(clusterNb, i);
		}
		
		return reducerNum;
	}

}
