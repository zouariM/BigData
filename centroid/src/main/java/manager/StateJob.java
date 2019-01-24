package manager;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileStatus;

import writable.PointWritable;

public class StateJob extends Configured implements Tool{
	
	private static final double epsilon = 0.001d;
	
	public static List<PointWritable> getCentroids(Path path, Comparator<PointWritable> comparator) throws IOException{
		List<PointWritable > centroids = new ArrayList<>();
		FileSystem fs = FileSystem.get(new Configuration());
		
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
		if(comparator != null)
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
		
		if(!fs.exists(old))
			throw new FileNotFoundException(old.getName());
		
		
		if(!fs.exists(newP))
			throw new FileNotFoundException(newP.toString());
		
		Comparator<PointWritable> comparator = (v1, v2) -> {
			int levels = v1.getLevels();
			for(int i=0; i<levels; i++) {
				IntWritable l = new IntWritable(i);
				IntWritable c1 = v1.getCluster(l);
				IntWritable c2 = v2.getCluster(l);
				
				if(!c1.equals(c2))
					return c1.compareTo(c2);
			}
			
			return 0;
		};
		List<PointWritable> oldC = getCentroids(old, comparator);
		List<PointWritable> newC = getCentroids(newP, comparator);
		
		fs.delete(old, true);
		fs.rename(newP, old);
		
		System.out.println("old");
		oldC.forEach(System.out::println);
		System.out.println("new");
		newC.forEach(System.out::println);
		
		for(int i=0; i<oldC.size(); i++)
			if(oldC.get(i).distanceTo(newC.get(i)) >= epsilon)
				return -1;
		
		return 0;
	}
	
}
