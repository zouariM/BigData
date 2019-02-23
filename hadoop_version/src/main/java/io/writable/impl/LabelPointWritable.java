package io.writable.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import io.writable.ClusterWritable;

public class LabelPointWritable implements ClusterWritable<LabelPointWritable>{
	
	private PointWritable point;
	private DoubleWritable metering;
	private Text label;
	
	public LabelPointWritable() {
		point = new PointWritable();
		metering = new DoubleWritable();
		label = new Text();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		point.write(out);
		metering.write(out);
		label.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		point.readFields(in);
		metering.readFields(in);
		label.readFields(in);
	}

	@Override
	public int compareTo(LabelPointWritable o) {
		int c = this.point.compareTo(o.point);
		return (c == 0) ? this.metering.compareTo(o.metering) : c;
	}
	
	@Override
	public LabelPointWritable clone() {
		LabelPointWritable labelPoint = null;
		
		try {
			labelPoint = (LabelPointWritable) super.clone();
			labelPoint.point = this.point.clone();
			labelPoint.metering = new DoubleWritable(this.metering.get());
			labelPoint.label = new Text(this.label.toString());
		} catch (CloneNotSupportedException e) {
			// Impossible
			e.printStackTrace();
		}
		
		return labelPoint;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((metering == null) ? 0 : metering.hashCode());
		result = prime * result + ((point == null) ? 0 : point.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LabelPointWritable other = (LabelPointWritable) obj;
		if (metering == null) {
			if (other.metering != null)
				return false;
		} else if (!metering.equals(other.metering))
			return false;
		if (point == null) {
			if (other.point != null)
				return false;
		} else if (!point.equals(other.point))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return String.format("%s %s + {label: %s, metering: %s}", 
				this.getClass().getSimpleName(), point, label, metering);
	}

	@Override
	public void parseLine(String line, int[] columns) {
		if(columns.length < 3)
			throw new IllegalArgumentException(String.format("columns input size = %s < 3", columns.length));

		this.point.parseLine(line, Arrays.copyOfRange(columns, 2, columns.length));
		
		int meteringCloumn = columns[0];
		int labelColumn = columns[1];
		String fields[] = line.split(",");
		
		try {
			this.label.set(fields[labelColumn]);
			this.metering.set(Double.parseDouble(fields[meteringCloumn]));
		}
		catch(NumberFormatException | ArrayIndexOutOfBoundsException ex) {
			throw new IllegalArgumentException(ex.getMessage());
		}
	}

	@Override
	public PointWritable getPoint() {
		return this.point;
	}
	
	public String getLabel() {
		return this.label.toString();
	}
	
	public Double getMetering() {
		return this.metering.get();
	}
}
