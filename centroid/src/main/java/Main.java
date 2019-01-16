import java.util.StringJoiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.ToolRunner;

import centroid.SetCentroidsJob;
import init.SetInitCentroidsJob;
import init.PointsParserJob;
import manager.StateJob;
import result.ResultJob;

public class Main {
	
	public static void main(String[] args) throws Exception {
		// Sequence file of points
		StringJoiner str = new StringJoiner(",");
		for(int i=3; i<args.length; i++)
			str.add(args[i]);
		String args1[] = {
				args[0],
				"/users/tests/seqfile",
				str.toString()
		};
		ToolRunner.run(new PointsParserJob(), args1);
		
		// Initialisation of centroids
		String seqFile = args1[1] + "/part-r-00000";
		String args2[] = {
				seqFile,
				"/users/tests/centroids_old.ser",
				args[2]
		};
		ToolRunner.run(new SetInitCentroidsJob(), args2);
		
		// Set centroids loop
		String args3[] = {
			seqFile,
			"/users/tests/res",
			args2[1],
			"/users/tests/centroids_new.ser",
			args[2]
		};
		String args4[] = {
				args2[1],
				args3[3]
		};
		boolean cond;
		
		do {
			// Set centroids iteration
			ToolRunner.run(new SetCentroidsJob(), args3);
			// Stop condition
			cond = (ToolRunner.run(new StateJob(), args4) == 0) ? true:false;
			System.out.println(cond);
		}while(!cond);
		
		// Put cluster number for each line
		String args5[] = {
				args[0],
				args[1],
				"/users/tests/res2",
				str.toString(),
				"/users/tests/centroids_old.ser",
		};
		ToolRunner.run(new ResultJob(), args5);
	}
}
