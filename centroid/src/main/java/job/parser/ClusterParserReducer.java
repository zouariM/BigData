package job.parser;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import io.writable.ClusterWritable;

public class ClusterParserReducer<T extends ClusterWritable<T>> extends Reducer<T, NullWritable, T, NullWritable>{
	
	private static final NullWritable outValue = NullWritable.get();
	
	@Override
	public void reduce(T key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, outValue);
	}
	
}
