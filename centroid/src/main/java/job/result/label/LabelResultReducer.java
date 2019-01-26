package job.result.label;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import io.writable.impl.LabelPointWritable;

public class LabelResultReducer extends Reducer<NullWritable, LabelPointWritable, NullWritable, Text>
{
	private final Map<String, LabelPointWritable> labelPointMax = new HashMap<>();
	private static final NullWritable outKey = NullWritable.get();
	private final Comparator<LabelPointWritable> compartor = new Comparator<LabelPointWritable>() {
		
		@Override
		public int compare(LabelPointWritable o1, LabelPointWritable o2) {
			return o1.getMetering().compareTo(o2.getMetering());
		}
	};
	
	@Override
	public void reduce(NullWritable key, Iterable<LabelPointWritable> values, Context context) {
		for(LabelPointWritable point:values) {
			String id = point.getPoint().getClusterId();
			LabelPointWritable max = labelPointMax.get(id);
			
			max = (max == null) ? point.clone() : ((compartor.compare(max, point) < 0) ? point.clone() : max);
			labelPointMax.put(id, max);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for(LabelPointWritable max:labelPointMax.values()) {
			String result = String.format("%s,%s,%s", max.getPoint().getClusterId(), 
					max.getLabel(), 
					max.getMetering());
			
			context.write(outKey, new Text(result));
		}
	}
}
