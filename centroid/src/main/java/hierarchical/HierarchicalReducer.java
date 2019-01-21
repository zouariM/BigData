package hierarchical;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import writable.PointWritable;

public class HierarchicalReducer extends Reducer<PointWritable, NullWritable, PointWritable, NullWritable>{
	
	@Override
	public void reduce(PointWritable key, Iterable<NullWritable> values, Context context) 
			throws IOException, InterruptedException 
	{
		Iterator<NullWritable> it = values.iterator();
		if(it.hasNext())
			context.write(key, it.next());
	}
}
