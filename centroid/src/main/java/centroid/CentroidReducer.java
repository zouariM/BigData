package centroid;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import writable.AvgWritable;
import writable.PointWritable;

public class CentroidReducer extends CentroidReducerTask<PointWritable>
{
	private AvgWritable centroid;
	
	@Override
	protected void writeContext(Context context, AvgWritable centroid) throws IOException, InterruptedException 
	{
		centroid.setMoy();
		this.centroid = centroid;
		context.write(NULL_KEY, centroid.getSum());
	}	
	
	@Override
	protected void cleanup(Context context) throws IOException {
		Path centroidPath = new Path(context.getConfiguration().get(SetCentroidsJob.NEW_CENTROIDS_PATH_KEY)); 
		FileSystem fs = FileSystem.get(context.getConfiguration());	
		DataOutputStream out = new DataOutputStream(fs.append(centroidPath));
		centroid.getSum().write(out);	
		out.flush();
		out.close();
		
		fs.close();
	}
}