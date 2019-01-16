package init;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import writable.PointWritable;

public class PointsParserReducer extends Reducer<PointWritable, NullWritable, PointWritable, NullWritable>{
	
	private static final NullWritable outValue = NullWritable.get();
	
	@Override
	public void reduce(PointWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, outValue);
	}
	
}
