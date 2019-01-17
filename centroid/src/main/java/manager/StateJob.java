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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Tool;

import writable.PointWritable;

public class StateJob extends Configured implements Tool{
	
	// TODO: a parametrer
	private static double EBSELON = 0.05;
	
	/**
	 * This should work as a replacement for the old getCentroids, it can work on both 
	 * a single centroid file, or a directory containing job output files 
	 * juste 3malt il fonction, ma sta3malthach w ma tastithach 
	 */
	private List<PointWritable> getCentroids(Path path, Comparator<PointWritable> comparator) throws IOException{
		List<PointWritable > centroids = new ArrayList<>();
		FileSystem fs = FileSystem.get(getConf());
		
		if(fs.isDirectory(path)) {
			for(FileStatus s : fs.listStatus(path,(p) -> p.getName().startsWith("part")))  
					centroids.addAll(getCentroids(s.getPath(),comparator));
		}else {
			DataInputStream reader = new DataInputStream(fs.open(path));
			while(reader.available() > 0) {
				PointWritable c = new PointWritable();
				c.readFields(reader);
				centroids.add(c);
			}
			reader.close();
		}
		Collections.sort(centroids, comparator);
		return centroids;
	}
	
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

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		if(!fs.exists(old) || !fs.exists(newP))
			throw new FileNotFoundException();
		
		Comparator<PointWritable> comparator = (v1, v2) -> v1.getClusterId().compareTo(v2.getClusterId());
		List<PointWritable> oldC = getCentroids(fs.open(old), comparator);
		List<PointWritable> newC = getCentroids(fs.open(newP), comparator);
		
		fs.delete(old, false);
		fs.rename(newP, old);
		fs.close();
		
		return checkStopCondition(oldC,newC);
	}
	
	private int checkStopCondition(List<PointWritable> oldC, List<PointWritable> newC) {
		for(int i=0; i<oldC.size(); i++)
			if(oldC.get(i).distanceTo(newC.get(i)).doubleValue() > EBSELON)
				return -1;
		return 0;
	}
	
}
