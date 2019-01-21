package writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringJoiner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PointWritable implements WritableComparable<PointWritable>, Cloneable{

	private DoubleArrayWritable coordinates;
	private MapWritable clusters;
	
	public PointWritable() {
		super();
		coordinates = new DoubleArrayWritable();
		clusters = new MapWritable();
	}
	
	public PointWritable(String str, int columns[]){
		String data[] = str.split(",");
		int dimension = columns.length;
		double coordinates[] = new double[dimension];

		for(int i=0; i<dimension; i++)
			try {
				int index = columns[i];
				coordinates[i] = Double.parseDouble(data[index]);				
				}
			catch(NumberFormatException | ArrayIndexOutOfBoundsException ex) {
					throw new IllegalArgumentException(str);
				}
		
		this.coordinates = new DoubleArrayWritable(coordinates);
		this.clusters = new MapWritable();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		coordinates.write(out);
		clusters.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		coordinates.readFields(in);
		clusters.readFields(in);
	}

	public int getDimension() {
		return coordinates.length();
	}
	
	public int getLevels() {
		return clusters.size();
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

	public IntWritable getCluster(IntWritable level) {
		return (IntWritable)clusters.get(level);
	}
	
	public IntWritable getBelowest() {
		int l = getLevels();
		return getCluster(new IntWritable(l-1));
	}
	
	public MapWritable getClusters() {
		return new MapWritable(clusters);
	}
	
	public void setCluster(IntWritable level, IntWritable id) {
		clusters.put(level, id);
	}
	
	public void setClusters(MapWritable clusters) {
		this.clusters = new MapWritable(clusters);
	}
	
	public void copyClusters(PointWritable point) {
		this.clusters = new MapWritable(point.clusters);
	}
	
	public PointWritable devide(int n) {
		PointWritable moy = this.clone();
		for(DoubleWritable d:moy.coordinates)
			d.set(d.get()/n);
		
		return moy;
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
		StringJoiner str = new StringJoiner(",");
		for(Writable level:clusters.keySet())
			str.add(String.format("%s:%s", (IntWritable)level, (IntWritable)clusters.get(level)));
		
		return "Point [dimension=" + coordinates.length() 
				+ ", coordinates=" + coordinates 
				+ ", clusters= "+str.toString()+"]";
	}
	
	public PointWritable clone() {
		PointWritable point = null;
		
		try {
			point = (PointWritable) super.clone();
			point.coordinates = coordinates.clone();
			point.clusters = new MapWritable(clusters);
			
		} catch (CloneNotSupportedException e) {
			// Impossible
			e.printStackTrace();
		}
		
		return point;
	}
}
