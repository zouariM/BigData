package job.centroid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import io.writable.AvgWritable;

public abstract class ClusterCentroidReducerTask<VALUEOUT>extends Reducer<NullWritable, AvgWritable, NullWritable, VALUEOUT>
{	
	protected static final NullWritable NULL_KEY = NullWritable.get();
	
	protected abstract void writeContext(Context context, AvgWritable centroid)throws IOException, InterruptedException;
	
	@Override
	public final void  reduce(NullWritable key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException 
	{	
		List<AvgWritable> list= new ArrayList<>();
		
		for(AvgWritable avg:values) {
			AvgWritable aux = avg.clone();
			list.add(aux);
		}
		
		AvgWritable centroid = list.stream().reduce((v1,v2)->{
			AvgWritable res = v1.clone();
			res.add(v2);
			return res;
		}).orElse(null);
		
		this.writeContext(context, centroid);
	}
}