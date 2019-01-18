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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FileStatus;

import writable.PointWritable;

public class StateJob extends Configured implements Tool{
	
	public static List<PointWritable> getCentroids(Path path, Comparator<PointWritable> comparator) throws IOException{
		System.out.println("path = " + path);
		List<PointWritable > centroids = new ArrayList<>();
		FileSystem fs = FileSystem.get(new Configuration());
		
		if(fs.isDirectory(path)) {
			System.out.println("parsing dir");
			for(FileStatus s : fs.listStatus(path,(p) -> p.getName().startsWith("part"))){  
				System.out.println("parsing file " + s.getPath());
				centroids.addAll(getCentroids(s.getPath(),comparator));
			}
		}else {
			System.out.println("parsing file");
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
		
		Comparator<PointWritable> comparator = (v1, v2) -> v1.getClusterId().compareTo(v2.getClusterId());
		List<PointWritable> oldC = getCentroids(old, comparator);
		List<PointWritable> newC = getCentroids(newP, comparator);
		
		fs.delete(old, true);
		fs.rename(newP, old);
		
		System.out.println("old");
		oldC.forEach(System.out::println);
		System.out.println("new");
		newC.forEach(System.out::println);
		
		for(int i=0; i<oldC.size(); i++)
			if(! oldC.get(i).equals(newC.get(i)))
				return -1;
		
		return 0;
	}
	
	
}
