import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import io.writable.impl.LabelPointWritable;
import job.centroid.ClusterCentroidJob;
import job.centroid.SetInitCentroidsJob;
import job.centroid.StateJob;
import job.centroid.impl.PointCentroidJob;
import job.hierarchical.impl.LabelPointHierarchicalJob;
import job.parser.impl.LabelPointParserJob;
import job.result.label.LabelResultJob;

public class Main {
	
	private static final String JOB_DIR = "/users/tests/";
	private static final String SEQ_FOLDER_PATH = JOB_DIR + "seqfiles";
	private static final String OLD_CENTROIDS_PATH = JOB_DIR + "old_centroids";
	private static final String NEW_CENTROIDS_PATH = JOB_DIR + "new_centroids";
	private static final String LABEL_RESULT_PATH = JOB_DIR + "label";
	
	public static void main(String[] args) throws Exception {
		if(args.length < 5) {
			System.err.println("Invalid arguments, try: <inputPath> <ouputPath> <clusterNb> <levelsNb> <columns>");
			System.exit(-1);
		}
		
		// Sequence file of points
		StringJoiner str = new StringJoiner(",");
		for(int i=4; i<args.length; i++)
			str.add(args[i]);
		Integer levels = Integer.parseInt(args[3]);
		Integer clusterNb = Integer.parseInt(args[2]);
		
		String args1[] = {
				args[0],
				SEQ_FOLDER_PATH,
				str.toString()
		};
		//ToolRunner.run(new PointParserJob(), args1);
		ToolRunner.run(new LabelPointParserJob(), args1);
		
		FileSystem fs = FileSystem.get(new Configuration());
		Path seq = new Path(SEQ_FOLDER_PATH);
		Path seq2 = new Path(SEQ_FOLDER_PATH+"2");
		
		for(Integer i=0; i<levels; i++) {
			// Initialisation of centroids
			String args2[] = {
					SEQ_FOLDER_PATH,
					OLD_CENTROIDS_PATH,
					clusterNb.toString()
			};	
			try{
				//ToolRunner.run(new SetInitCentroidsJob<PointWritable>(PointWritable.class), args2);
				ToolRunner.run(new SetInitCentroidsJob<LabelPointWritable>(LabelPointWritable.class), args2);
			}
			catch(IllegalStateException ex) {
				ex.printStackTrace();
				break;
			}
			
			// Set centroids loop
			ClusterCentroidJob centroidsJob = new PointCentroidJob();			
			// State job
			StateJob stateJob = new StateJob();
			
			boolean cond;			
			do {
				// Set centroids iteration
				String args3[] = {
						SEQ_FOLDER_PATH,
						NEW_CENTROIDS_PATH,
						OLD_CENTROIDS_PATH,
						clusterNb.toString(),
						new Integer(i+1).toString()
					};
				ToolRunner.run(centroidsJob, args3);
				
				// Stop condition
				String args4[] = {
						OLD_CENTROIDS_PATH,
						NEW_CENTROIDS_PATH
				};
				cond = (ToolRunner.run(stateJob, args4) == 0) ? true:false;
				System.out.println(cond);
			}while(!cond);
			
			String args5[] = {
					SEQ_FOLDER_PATH,
					SEQ_FOLDER_PATH+"2",
					OLD_CENTROIDS_PATH,
					clusterNb.toString(),
					new Integer(i+1).toString()
			};
			//ToolRunner.run(new PointHierarchicalJob(), args5);
			ToolRunner.run(new LabelPointHierarchicalJob(), args5);
			
			fs.delete(seq, true);
			fs.rename(seq2, seq);
			
			String args7[] = {
				SEQ_FOLDER_PATH,
				LABEL_RESULT_PATH
			};
			ToolRunner.run(new LabelResultJob(), args7);
			Path resultPath = new Path(args[1]+"_"+i);
			if(fs.exists(resultPath))
				fs.delete(resultPath, true);
			
			Path jobResultPath = fs.listStatus(new Path(LABEL_RESULT_PATH), p->p.getName().startsWith("part"))[0].getPath();
			fs.rename(jobResultPath, resultPath);
		}

		// Put cluster number for each line
		/*String args6[] = {
				SEQ_FOLDER_PATH,
				args[1],
				str.toString(),
				OLD_CENTROIDS_PATH,
				levels.toString()
		};
		ToolRunner.run(new ResultJob(), args6);*/

	}
}
