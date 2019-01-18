package centroid;

import java.io.IOException;

import writable.AvgWritable;
import writable.PointWritable;

public class CentroidReducer extends CentroidReducerTask<PointWritable>
{	
	@Override
	protected void writeContext(Context context, AvgWritable centroid) throws IOException, InterruptedException 
	{
		context.write(NULL_KEY, centroid.getMoy());
	}	
}