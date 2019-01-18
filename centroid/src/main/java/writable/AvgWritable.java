package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class AvgWritable implements WritableComparable<AvgWritable>, Cloneable{
	
	private IntWritable nb;
	private IntWritable k;
	private PointWritable sum;
	
	public AvgWritable() {
		nb = new IntWritable();
		k = new IntWritable();
		sum = new PointWritable();
	}

	public void setK(IntWritable k) {
		this.k = new IntWritable(k.get());
	}
	
	public AvgWritable(PointWritable vector) {
		this.k = new IntWritable(vector.getClusterId());
		this.nb = new IntWritable(1);
		
		this.sum = vector;
	}
	
	public IntWritable getK() {
		return k;
	}
	
	public void add(AvgWritable avg) {
		if(k.equals(avg.k) == false)
			throw new IllegalArgumentException(String.format("%s != %s", k, avg.k));
		
		nb.set(nb.get()+avg.nb.get());
		sum.add(avg.sum);
	}
	
	public void add(PointWritable vector) {
		if(k.equals(vector.getClusterId()) == false)
			throw new IllegalArgumentException();
		
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
		k.write(out);
		nb.write(out);
		sum.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		k.readFields(in);
		nb.readFields(in);
		sum.readFields(in);
	}

	@Override
	public int compareTo(AvgWritable o) {
		return k.compareTo(o.k);
	}

	@Override
	public int hashCode() {
		return k.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AvgWritable other = (AvgWritable) obj;
		if (k == null) {
			if (other.k != null)
				return false;
		} else if (!k.equals(other.k))
			return false;
		return true;
	}
	
	public AvgWritable clone() {
		AvgWritable avg = new AvgWritable();
		avg.k = k;
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
		return String.format("centroid : %s ** cluster = %s ** nb = %s", sum.toString(), k.toString(), nb.toString());
	}
}
