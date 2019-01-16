package centroid;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import writable.AvgWritable;
import writable.PointWritable;

public class CentroidMapper extends Mapper<PointWritable, NullWritable, NullWritable, AvgWritable>{
	
	private final Set<PointWritable> centroids = new HashSet<>();
	private static final NullWritable nullKey = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path centroidPath = new Path(context.getConfiguration().get(SetCentroidsJob.OLD_CENTROIDS_PATH_KEY)); 
		DataInputStream in = new DataInputStream(fs.open(centroidPath)); 

		while(in.available() > 0) {
			PointWritable c = new PointWritable();
			c.readFields(in);
			centroids.add(c);
		}
		in.close();
		
		int k = conf.getInt(SetCentroidsJob.CLUSTER_NBR_KEY, 0);
		if(centroids.size() != k)
			throw new IllegalStateException(String.format("clusterNb %s < centroids %s", k, centroids.size()));
	}
	
	@Override
	public void map(PointWritable key, NullWritable value, Context context) throws IOException, InterruptedException {		
		Optional<PointWritable> op = centroids.stream().min((v1,v2)->v1.distanceTo(key).compareTo(v2.distanceTo(key)));
		
		if(op.isPresent()) {
			PointWritable centroid = op.get();
			int k = centroid.getClusterId();			
			key.setK(k);
			AvgWritable avg = new AvgWritable(key);;
			
			context.write(nullKey, avg);
		}
	}

}
