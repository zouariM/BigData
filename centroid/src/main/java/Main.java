import java.util.StringJoiner;

import org.apache.hadoop.util.ToolRunner;

import centroid.SetCentroidsJob;
import init.SetInitCentroidsJob;
import init.PointsParserJob;
import manager.StateJob;
import result.ResultJob;

public class Main {
	
	private static final String JOB_DIR = "/users/mustapha/tests/";
	private static final String SEQ_FILE_PATH = JOB_DIR + "seqfile";
	private static final String OLD_CENTROIDS_PATH = JOB_DIR + "old_centroids";
	private static final String NEW_CENTROIDS_PATH = JOB_DIR + "new_centroids";
	private static final String JOB_RESULT_PATH = JOB_DIR + "result";
	
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
			NEW_CENTROIDS_PATH,
			OLD_CENTROIDS_PATH,
			args[2]
		};
		String args4[] = {
				OLD_CENTROIDS_PATH,
				NEW_CENTROIDS_PATH
		};
		boolean cond;
		
		do {
			// Set centroids iteration
			System.out.println("before entering");
			ToolRunner.run(new SetCentroidsJob(), args3);
			System.out.println("done from set centroids");
			// Stop condition
			
			cond = (ToolRunner.run(new StateJob(), args4) == 0) ? true:false;
			System.out.println(cond);
		}while(!cond);
		
		// Put cluster number for each line
		String args5[] = {
				args[0],
				JOB_RESULT_PATH,
				args[1],
				str.toString(),
				OLD_CENTROIDS_PATH,
		};
		ToolRunner.run(new ResultJob(), args5);
	}
}
