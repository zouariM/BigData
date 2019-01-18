package manager;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

import writable.PointWritable;

public class StateJob extends Configured implements Tool{
	
	private List<PointWritable> getCentroids(InputStream in, Comparator<PointWritable> comparator) throws IOException
	{
		List<PointWritable> centroids = new ArrayList<>();
		DataInputStream reader = new DataInputStream(in);
		
		while(reader.available() > 0) {
			PointWritable c = new PointWritable();
			c.readFields(reader);
			centroids.add(c);
		}
		
		reader.close();
		Collections.sort(centroids, comparator);
		
		return centroids;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Invalid arguments, try: <oldCentroidsPath> <newCentroidsPath>");
			return -1;
		}
		
		Path old = new Path(args[0]);
		Path newP = new Path(args[1]);
		
		System.out.println(old);
		System.out.println(newP);

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		if(!fs.exists(old) || !fs.exists(newP))
			throw new FileNotFoundException();
		
		Comparator<PointWritable> comparator = (v1, v2) -> v1.getClusterId().compareTo(v2.getClusterId());
		List<PointWritable> oldC = getCentroids(fs.open(old), comparator);
		List<PointWritable> newC = getCentroids(fs.open(newP), comparator);
		
		fs.delete(old, false);
		fs.rename(newP, old);
		
		oldC.forEach(System.out::println);
		newC.forEach(System.out::println);
		
		for(int i=0; i<oldC.size(); i++)
			if(! oldC.get(i).equals(newC.get(i)))
				return -1;
		
		return 0;
	}
	
	
}
