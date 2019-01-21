package centroid;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import manager.StateJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import writable.AvgWritable;
import writable.PointWritable;

public class CentroidMapper extends Mapper<PointWritable, NullWritable, NullWritable, AvgWritable>{
	
	private List<PointWritable> centroids;
	private static final NullWritable nullKey = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path centroidPath = new Path(context.getConfiguration().get(CentroidsJob.OLD_CENTROIDS_PATH_KEY));
		centroids = StateJob.getCentroids(centroidPath, null);

		int nb = conf.getInt(CentroidsJob.CENTROIDS_NB_KEY, 0);
		
		if(centroids.size() != nb)
			throw new IllegalStateException(String.format("clusterNb %s != centroids %s file: %s", 
					nb, centroids.size(), centroidPath));
	}
	
	@Override
	public void map(PointWritable key, NullWritable value, Context context) throws IOException, InterruptedException {	
		final MapWritable clusters = key.getClusters();
		Optional<PointWritable> op = centroids.stream()
				.filter(v->{
					MapWritable map = v.getClusters();
					for(Writable k:clusters.keySet()) {
						IntWritable x = (IntWritable)clusters.get(k);
						IntWritable y = (IntWritable)map.get(k);
						if(!x.equals(y))
							return false;
					}
							
					return true;
				})
				.min((v1,v2)->v1.distanceTo(key).compareTo(v2.distanceTo(key)));
		
		if(op.isPresent()) {
			PointWritable centroid = op.get();
			key.setClusters(centroid.getClusters());
			AvgWritable avg = new AvgWritable(key);;
			
			context.write(nullKey, avg);
		}
	}

}
