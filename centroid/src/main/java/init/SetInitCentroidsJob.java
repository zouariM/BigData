package init;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;

import writable.PointWritable;

public class SetInitCentroidsJob extends Configured implements Tool{
	
	private Set<PointWritable> getCentroids(Path file, int nb) throws IOException{
		FileSystem fs = FileSystem.get(getConf());
		if(!fs.exists(file))
			throw new FileNotFoundException(String.format("%s does not exist", file));
		if(!fs.isFile(file))
			throw new IllegalArgumentException(String.format("%s is not file", file));
				
		SequenceFile.Reader reader = new SequenceFile.Reader(getConf(), SequenceFile.Reader.file(file));
		PointWritable v = new PointWritable();
		Set<PointWritable> centroids = new HashSet<>();
		
		while(reader.next(v) && centroids.size()<nb) {
			int level = v.getLevels();
			v.setCluster(new IntWritable(level), new IntWritable(centroids.size()));
			centroids.add(v);	
			v = new PointWritable();
		}
		
		reader.close();
		
		if(centroids.size() < nb) 
			throw new IllegalStateException(String.format("Valid lines %s less then cluster number %s, file %s", 
											centroids.size(), nb, file));
		return centroids;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Invalid arguments, use: <inputPath> <centroidPath> <nbVector>");
			return -1;
		}
		
		Path inputPath = new Path(args[0]);
		Path centroidPath = new Path(args[1]);
		int k = Integer.parseInt(args[2]);
		
		FileSystem fs = FileSystem.get(getConf());
		
		if(!fs.exists(inputPath)) {
			System.err.println(String.format("%s does not exist", inputPath));
			return -1;
		}
		
		if(fs.exists(centroidPath))
			fs.delete(centroidPath, true);
		
		Set<PointWritable> centroids = new HashSet<>();
		for(FileStatus s : fs.listStatus(inputPath,(p) -> p.getName().startsWith("part")))
			centroids.addAll(getCentroids(s.getPath(), k));
		
		System.out.println("centroids initialisation");
		centroids.forEach(System.out::println);
		
		DataOutputStream out = new DataOutputStream(fs.create(centroidPath));
		for(PointWritable vw:centroids)
			vw.write(out);
		out.flush();
		
		out.close();
		return 0;
	}

}
