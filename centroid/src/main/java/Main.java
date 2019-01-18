import java.util.StringJoiner;

import org.apache.hadoop.util.ToolRunner;

import centroid.SetCentroidsJob;
import init.SetInitCentroidsJob;
import init.PointsParserJob;
import manager.StateJob;
import result.ResultJob;

public class Main {
	
	private static final String JOB_DIR = "/users/tests/";
	private static final String SEQ_FILE_PATH = JOB_DIR + "seqfile";
	private static final String OLD_CENTROIDS_PATH = JOB_DIR + "centroids_old.ser";
	private static final String NEW_CENTROIDS_PATH = JOB_DIR + "centroids_new.ser";
	private static final String JOB_RESULT_PATH = JOB_DIR + "result";
	private static final String JOB_SET_CENTROIDS_PATH = JOB_DIR + "setcentroids";
	
	public static void main(String[] args) throws Exception {
		// Sequence file of points
		StringJoiner str = new StringJoiner(",");
		for(int i=3; i<args.length; i++)
			str.add(args[i]);
		String args1[] = {
				args[0],
				SEQ_FILE_PATH,
				str.toString()
		};
		ToolRunner.run(new PointsParserJob(), args1);
		
		// Initialisation of centroids
		String seqFile = SEQ_FILE_PATH + "/part-r-00000";
		String args2[] = {
				seqFile,
				OLD_CENTROIDS_PATH,
				args[2]
		};
		ToolRunner.run(new SetInitCentroidsJob(), args2);
		
		// Set centroids loop
		String args3[] = {
			seqFile,
			JOB_SET_CENTROIDS_PATH,
			OLD_CENTROIDS_PATH,
			NEW_CENTROIDS_PATH,
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
				JOB_RESULT_PATH,
				str.toString(),
				OLD_CENTROIDS_PATH,
		};
		ToolRunner.run(new ResultJob(), args5);
	}
}
