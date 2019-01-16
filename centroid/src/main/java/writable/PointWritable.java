package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable>, Cloneable{

	private DoubleArrayWritable coordinates;	
	private IntWritable clusterId;
	
	public PointWritable() {
		super();
		coordinates = new DoubleArrayWritable();
		clusterId = new IntWritable();
	}
	
	public PointWritable(String str, int columns[]){
		clusterId = new IntWritable();
		
		String data[] = str.split(",");
		int dimension = columns.length;
		double coordinates[] = new double[dimension];

		for(int i=0; i<dimension; i++)
			try {
				int index = columns[i];
				coordinates[i] = Double.parseDouble(data[index]);				
				}
			catch(NumberFormatException | ArrayIndexOutOfBoundsException ex) {
					throw new IllegalArgumentException();
				}
		
		this.coordinates = new DoubleArrayWritable(coordinates);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		coordinates.write(out);
		clusterId.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		coordinates.readFields(in);
		clusterId.readFields(in);
	}

	public int getDimension() {
		return coordinates.length();
	}
	
	@Override
	public int compareTo(PointWritable arg0) {
		if(getDimension() != arg0.getDimension())
			throw new IllegalArgumentException("Points must have same dimension");
		
		int l = coordinates.length();
		for(int i=0; i<l; i++) {
			int r = coordinates.get(i).compareTo(arg0.coordinates.get(i));
			if(r != 0)
				return r;
		}
		
		return 0;
	}

	public void add(PointWritable point) {
		int l = coordinates.length();
		double tab[] = new double[l];
		
		for(int i=0; i<l; i++) 
			tab[i] = point.coordinates.get(i).get() + this.coordinates.get(i).get();
		
		this.coordinates.set(tab);;
	}
	
	public Double distanceTo(PointWritable v) {
		double distance = 0d;
		int l = coordinates.length();
		
		for(int i=0; i<l; i++) {
			double diff = coordinates.get(i).get() - v.coordinates.get(i).get();
			distance += Math.pow(diff, 2d);
		}
		
		return new Double(Math.sqrt(distance));
	}

	public Integer getClusterId() {
		return clusterId.get();
	}
	
	public void setK(int k) {
		this.clusterId.set(k);
	}
	
	public void setK(IntWritable k) {
		this.clusterId = k;
	}
	
	public void devide(int n) {
		for(DoubleWritable d:coordinates)
			d.set(d.get()/n);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PointWritable other = (PointWritable) obj;
		return coordinates.equals(other.coordinates);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((coordinates == null) ? 0 : coordinates.hashCode());
		
		return result;
	}
	
	@Override
	public String toString() {
		return "Point [dimension=" + coordinates.length() 
				+ ", coordinates=" + coordinates 
				+ ", cluster= "+clusterId+"]";
	}
	
	public PointWritable clone() {
		PointWritable point = null;
		
		try {
			point = (PointWritable) super.clone();
			point.coordinates = coordinates.clone(); 
		} catch (CloneNotSupportedException e) {
			// Impossible
			e.printStackTrace();
		}
		
		return point;
	}
}
