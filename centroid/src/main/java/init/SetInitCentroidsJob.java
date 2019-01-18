package init;

import java.io.DataOutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;

import writable.PointWritable;

public class SetInitCentroidsJob extends Configured implements Tool{

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
		
		SequenceFile.Reader reader = new SequenceFile.Reader(getConf(), SequenceFile.Reader.file(inputPath));
		PointWritable v = new PointWritable();
		Set<PointWritable> vectors = new HashSet<>();
		
		while(reader.next(v) && vectors.size()<k) {
			v.setK(vectors.size());
			vectors.add(v);	
			v = new PointWritable();
		}
		reader.close();
		
		if(vectors.size() < k) {
			System.err.println(String.format("Valid lines %s less then cluster number %s", vectors.size(), k));
			return -1;
		}
		vectors.forEach(System.out::println);
		
		DataOutputStream out = new DataOutputStream(fs.create(centroidPath));
		for(PointWritable vw:vectors)
			vw.write(out);
		out.flush();
		
		out.close();
		return 0;
	}

}
