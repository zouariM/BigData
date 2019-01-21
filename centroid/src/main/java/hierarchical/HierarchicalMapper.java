package hierarchical;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import manager.StateJob;
import writable.PointWritable;

public class HierarchicalMapper extends Mapper<PointWritable, NullWritable, PointWritable, NullWritable>{
	
	private List<PointWritable> centroids;
	private static final NullWritable nullValue = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();		
		Path path = new Path(conf.get(HierarchicalJob.CENTROIDS_PATH));
		centroids = StateJob.getCentroids(path, null);
	}

	@Override
	public void map(PointWritable point, NullWritable value, Context context) throws IOException, InterruptedException 
	{
		Optional<PointWritable> op = centroids.stream().min((v1,v2)->v1.distanceTo(point).compareTo(v2.distanceTo(point)));
		if(!op.isPresent())
			throw new IllegalStateException(String.format("%s does not have a centroid!", point));
		
		PointWritable centroid = op.get();
		point.copyClusters(centroid);
		
		context.write(point, nullValue);
	}
}
