package job.hierarchical;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import io.writable.ClusterWritable;

public class ClusterHierarchicalReducer<T extends ClusterWritable<T>> extends Reducer<T, NullWritable, T, NullWritable>{
	
	@Override
	public void reduce(T key, Iterable<NullWritable> values, Context context) 
			throws IOException, InterruptedException 
	{
		Iterator<NullWritable> it = values.iterator();
		if(it.hasNext())
			context.write(key, it.next());
	}
}
