package result;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import manager.StateJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import writable.PointWritable;

public class ResultMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	private List<PointWritable> centroids;
	private int columns[];
	private static final NullWritable nullKey = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();		
		columns = conf.getInts(ResultJob.COLUMNS_KEY);
		Path path = new Path(conf.get(ResultJob.CENTROIDS_PATH));
		
		centroids = StateJob.getCentroids(path, null);
	}

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String result = "";
		try {
			PointWritable point = new PointWritable(value.toString(), columns);
			Optional<PointWritable> op = centroids.stream().min((v1,v2)->v1.distanceTo(point).compareTo(v2.distanceTo(point)));
			
			if(op.isPresent()) {
				PointWritable centroid = op.get();		
				
				StringJoiner str = new StringJoiner(",");
				int levels = centroid.getLevels();
				for(int i=0; i<levels; i++)
					str.add(centroid.getCluster(new IntWritable(i)).toString());
				
				result = String.format("%s,%s", value.toString(), str);
				}
			}
			
		catch(IllegalArgumentException ex) {
			result = String.format("%s,%s", value.toString(), "?");
			}
		
		context.write(nullKey, new Text(result));
	}
	
}
