package hierarchical;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import writable.PointWritable;

public class HierarchicalPartitionner extends Partitioner<PointWritable, NullWritable>{
	
	@Override
	public int getPartition(PointWritable key, NullWritable value, int numPartitions) {
		MapWritable levels = key.getClusters();
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
