package job.result.label;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import io.writable.impl.LabelPointWritable;


public class LabelResultCombiner extends Reducer<NullWritable, LabelPointWritable, NullWritable, LabelPointWritable>
{
	private final Comparator<LabelPointWritable> compartor = new Comparator<LabelPointWritable>() {
		
		@Override
		public int compare(LabelPointWritable o1, LabelPointWritable o2) {
			return o1.getMetering().compareTo(o2.getMetering());
		}
	};

	@Override
	public void reduce(NullWritable key, Iterable<LabelPointWritable> values, Context context) 
			throws IOException, InterruptedException 
	{
		Iterator<LabelPointWritable> it = values.iterator();
		if(it.hasNext()) {
			LabelPointWritable max = it.next().clone();
			
			while(it.hasNext()) {
				LabelPointWritable aux = it.next();
				if(compartor.compare(aux, max) > 0)
					max = aux.clone();
			}
			
			context.write(key, max);
		}
	}
}
