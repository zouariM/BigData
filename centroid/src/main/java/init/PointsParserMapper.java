package init;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import writable.PointWritable;

public class PointsParserMapper extends Mapper<LongWritable, PointWritable, PointWritable, NullWritable>{
	
	private static final NullWritable outValue = NullWritable.get();
	
	@Override
	public void map(LongWritable key, PointWritable value, Context context) throws IOException, InterruptedException {
		context.write(value, outValue);
	}

}
