package centroid;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import writable.AvgWritable;

public class CentroidCombiner extends CentroidReducerTask<AvgWritable>{

	@Override
	protected void writeContext(Reducer<NullWritable, AvgWritable, NullWritable, AvgWritable>.Context context,
			AvgWritable centroid) throws IOException, InterruptedException {
		context.write(NULL_KEY, centroid);
	}
	
	
}
