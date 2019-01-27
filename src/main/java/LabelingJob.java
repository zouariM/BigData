import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import data.LabelPoint;
import scala.Tuple2;

public class LabelingJob implements Serializable{

	private static final long serialVersionUID = 1L;
	private int clusterNb;
	private int levelsNb;
	
	public void setClusterNb(int clusterNb) {
		this.clusterNb = clusterNb;
	}
	
	public void setLevels(int nb) {
		this.levelsNb = nb;
	}
	
	public List<JavaRDD<String>> run(JavaSparkContext sc, List<JavaRDD<LabelPoint>> clusters) {
		List<JavaRDD<String>> output = new ArrayList<>();
		List<JavaRDD<LabelPoint>> supClusters = new ArrayList<>();
		
		if(levelsNb == 0)
			return output;
		

		List<JavaRDD<String>> aux = new ArrayList<>();
		clusters.forEach((JavaRDD<LabelPoint> c)->{
			JavaRDD<LabelPoint> points = c.mapToPair(p->new Tuple2<>(p.getClusterPoint().getClusterId(clusterNb), p))
			 .reduceByKey((p1,p2)->{
				 if(p1.compareTo(p2) <0 )
					 return p2;
				 else 
					 return p1;
			 }).values();
			
			points = points.cache();
			
			aux.add(points.map(p->p.toString()));
			supClusters.add(points.map((LabelPoint p)->{
				p.getClusterPoint().decrementClustersLevel();
				return p;
			}));
		});
		
		JavaRDD<String> levelResult = sc.emptyRDD();
		for(JavaRDD<String> elt:aux)
			levelResult = sc.union(levelResult, elt);
		output.add(levelResult);
		
		LabelingJob labelingJob = new LabelingJob();
		labelingJob.setClusterNb(clusterNb);
		labelingJob.setLevels(levelsNb-1);
		
		output.addAll(labelingJob.run(sc, supClusters));
		
		return output;
	}
}
