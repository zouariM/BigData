package job.result.hierarichical;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import io.writable.impl.PointWritable;
import job.centroid.StateJob;

public class ResultMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
	
	private List<PointWritable> centroids;
	private int columns[];
	private int levelsNb;
	private static final NullWritable nullKey = NullWritable.get();
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();		
		columns = conf.getInts(ResultJob.COLUMNS_KEY);
		levelsNb = conf.getInt(ResultJob.LEVELS_NB_KEY, 0);
		
		Path path = new Path(conf.get(ResultJob.CENTROIDS_PATH));
		centroids = StateJob.getCentroids(path, null);
	}

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		String result = "";
		String ids[] = new String[levelsNb];
		int levelsV = 0;
		
		try {
			PointWritable point = new PointWritable();
			point.parseLine(value.toString(), columns);
			Optional<PointWritable> op = centroids.stream().min((v1,v2)->v1.distanceTo(point).compareTo(v2.distanceTo(point)));
			
			if(op.isPresent()) {
				PointWritable centroid = op.get();		
				
				levelsV = centroid.getLevels();
				for(int i=0; i<levelsV; i++)
					ids[i] = centroid.getCluster(new IntWritable(i)).toString();
				}
			}
			
		catch(IllegalArgumentException ex) {
			ex.printStackTrace();
			}
		
		finally {
			for(int i=levelsV; i<levelsNb; i++)
				ids[i] = "?";
			
			result = String.format("%s,%s", value, String.join(",", ids));
			}
		
		context.write(nullKey, new Text(result));
	}
	
}
