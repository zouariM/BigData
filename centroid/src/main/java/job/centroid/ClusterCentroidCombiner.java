package job.centroid;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import io.writable.AvgWritable;

public class ClusterCentroidCombiner extends ClusterCentroidReducerTask<AvgWritable>{

	@Override
	protected void writeContext(Reducer<NullWritable, AvgWritable, NullWritable, AvgWritable>.Context context,
			AvgWritable centroid) throws IOException, InterruptedException {
		context.write(NULL_KEY, centroid);
	}
	
	
}
