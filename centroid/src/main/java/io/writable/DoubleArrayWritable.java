package io.writable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringJoiner;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable implements Iterable<DoubleWritable>, Cloneable{

	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}
	
	public DoubleArrayWritable(double[] values) {
		super(DoubleWritable.class);
		set(values);
	}
	
	public void set(double values[]) {
		int l = values.length;
		Writable tab[] = new Writable[l];
		
		for(int i=0; i<l; i++)
			tab[i] = new DoubleWritable(values[i]);
		
		set(tab);
	}
	
	public DoubleWritable get(int i) {
		Writable values[] = get();
		if(i < 0 || i >= values.length)
			throw new ArrayIndexOutOfBoundsException();
		
		return (DoubleWritable)values[i];
	}

	@Override
	public Iterator<DoubleWritable> iterator() {
		Writable values[] = get();
		List<DoubleWritable> collection = new ArrayList<>();
		
		int length = values.length;
		for(int i=0; i<length; i++)
			collection.add((DoubleWritable)values[i]);
		
		return collection.iterator();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		for(DoubleWritable d:this)
			result = prime * result + ((d == null) ? 0 : d.hashCode());
		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj == this)
			return true;
		if(obj == null)
			return false;
		if(obj.getClass()!=getClass())
			return false;
		
		DoubleArrayWritable arrayW = (DoubleArrayWritable)obj;
		return Arrays.equals(get(), arrayW.get());
	}
	
	@Override
	public String toString() {
		StringJoiner joiner = new StringJoiner(" ");
		for(DoubleWritable d:this)
			joiner.add(d.toString());
		
		return joiner.toString();
	}

	public DoubleArrayWritable clone() {
		DoubleArrayWritable newI = null;
		
		try {
			newI = (DoubleArrayWritable)super.clone();
			int l = get().length;
			Writable values[] = new Writable[l];
			
			for(int i=0; i<l; i++)
				values[i] = new DoubleWritable(get(i).get());
			newI.set(values);
			
		} catch (CloneNotSupportedException e) {
			// Impossible
			e.printStackTrace();
		}
		
		return newI;
	}
	
	public int length() {
		return get().length;
	}
}
