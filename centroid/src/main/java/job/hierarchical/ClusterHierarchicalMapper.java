package job.hierarchical;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import io.writable.ClusterWritable;
import io.writable.impl.PointWritable;
import job.centroid.StateJob;

public class ClusterHierarchicalMapper<T extends ClusterWritable<T>> extends Mapper<T, NullWritable, T, NullWritable>{
	
	private List<PointWritable> centroids;
	private static final NullWritable nullValue = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();		
		Path path = new Path(conf.get(ClusterHierarchicalJob.CENTROIDS_PATH));
		centroids = StateJob.getCentroids(path, null);
	}

	@Override
	public void map(T key, NullWritable value, Context context) throws IOException, InterruptedException 
	{
		PointWritable point = key.getPoint();
		Optional<PointWritable> op = centroids.stream().min((v1,v2)->v1.distanceTo(point).compareTo(v2.distanceTo(point)));
		if(!op.isPresent())
			throw new IllegalStateException(String.format("%s does not have a centroid!", point));
		
		PointWritable centroid = op.get();
		point.copyClusters(centroid);
		
		context.write(key, nullValue);
	}
}
