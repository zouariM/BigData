package job.parser;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import io.writable.ClusterWritable;

public class ClusterParserMapper<T extends ClusterWritable<T>> extends Mapper<LongWritable, T, T, NullWritable>{
	
	private static final NullWritable outValue = NullWritable.get();
	
	@Override
	public void map(LongWritable key, T value, Context context) throws IOException, InterruptedException 
	{
		if(value != null)
			context.write(value, outValue);
	}

}
