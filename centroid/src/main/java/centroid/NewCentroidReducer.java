package centroid;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import writable.AvgWritable;
import writable.PointWritable;

public class NewCentroidReducer extends CentroidReducerTask<PointWritable>{

	@Override
	protected void writeContext(Reducer<NullWritable, AvgWritable, NullWritable, PointWritable>.Context context,
			AvgWritable centroid) throws IOException, InterruptedException {
		centroid.setMoy();
		context.write(NullWritable.get(), centroid.getSum());
	}

}
