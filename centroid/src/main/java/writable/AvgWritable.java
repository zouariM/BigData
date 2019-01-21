package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class AvgWritable implements Writable, Cloneable{
	
	private IntWritable nb;
	private PointWritable sum;
	
	public AvgWritable() {
		nb = new IntWritable();
		sum = new PointWritable();
	}

	public AvgWritable(PointWritable vector) {
		this.nb = new IntWritable(1);
		this.sum = vector;
	}
	
	public void add(AvgWritable avg) {
		nb.set(nb.get()+avg.nb.get());
		sum.add(avg.sum);
	}
	
	public void add(PointWritable vector) {
		nb.set(nb.get()+1);
		sum.add(vector);
	}
		
	public PointWritable getSum() {
		return sum;
	}

	public PointWritable getMoy() {
		return sum.devide(nb.get());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		nb.write(out);
		sum.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		nb.readFields(in);
		sum.readFields(in);
	}

	public AvgWritable clone() {
		AvgWritable avg = new AvgWritable();
		avg.nb = new IntWritable(nb.get());
		avg.sum = sum.clone();
	
		return avg;
	}
	
	public IntWritable getNb() {
		return nb;
	}

	public void setNb(IntWritable nb) {
		this.nb = nb;
	}

	public void setSum(PointWritable sum) {
		this.sum = sum;
	}

	@Override
	public String toString() {
		return String.format("centroid : %s ** cluster = %s ** nb = %s", sum.toString(), nb.toString());
	}
}
