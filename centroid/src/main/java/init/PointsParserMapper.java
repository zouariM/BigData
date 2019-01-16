package init;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import writable.PointWritable;

public class PointsParserMapper extends Mapper<LongWritable, Text, PointWritable, NullWritable>{
	
	private static final NullWritable outValue = NullWritable.get();
	private int[] columns;
	private Counter invalidCounter;
	
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		columns = conf.getInts(PointsParserJob.COLUMNS_ARG_KEY);
		invalidCounter = context.getCounter(PointsParserJob.PARSER_JOB_DATA_COUNTER,PointsParserJob.PARSER_JOB_INVALID_LINES_COUNTER);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			PointWritable vector = new PointWritable(value.toString(), columns);
			context.write(vector, outValue);
		}
		catch(IllegalArgumentException ex) {
			invalidCounter.increment(1);
			ex.printStackTrace();
		}
	}

}
