package result;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import writable.PointWritable;

public class ResultMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>{
	
	private final Set<PointWritable> centroids = new HashSet<>();
	private int columns[];
	private PrintWriter print;
	
	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		
		columns = conf.getInts(ResultJob.COLUMNS_KEY);
		Path path = new Path(conf.get(ResultJob.RESULT_PATH_KEY));
		print = new PrintWriter(fs.append(path));
		
		Path centroidPath = new Path(conf.get(ResultJob.CENTROIDS_PATH)); 
		DataInputStream in = new DataInputStream(fs.open(centroidPath)); 

		while(in.available() > 0) {
			PointWritable c = new PointWritable();
			c.readFields(in);
			centroids.add(c);
		}
		in.close();
	}

	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		String result = "";
		try {
			PointWritable point = new PointWritable(value.toString(), columns);
			Optional<PointWritable> op = centroids.stream().min((v1,v2)->v1.distanceTo(point).compareTo(v2.distanceTo(point)));
			
			if(op.isPresent()) {
				PointWritable centroid = op.get();
				int k = centroid.getClusterId();			
				
				result = String.format("%s,%s", value.toString(), k);
			}
			}
		
		catch(IllegalArgumentException ex) {
			result = String.format("%s,%s", value.toString(), "?");
		}
		
		print.println(result);
	}
	
	@Override
	protected void cleanup(Context context) {
		print.flush();
		print.close();
	}
}
