package job.centroid;

import java.io.IOException;

import io.writable.AvgWritable;
import io.writable.impl.PointWritable;

public class ClusterCentroidReducer extends ClusterCentroidReducerTask<PointWritable>
{	
	@Override
	protected void writeContext(Context context, AvgWritable centroid) throws IOException, InterruptedException 
	{
		context.write(NULL_KEY, centroid.getMoy());
	}	
}