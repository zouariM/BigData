import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaRDD;

import data.LabelPoint;
import scala.Tuple2;

public class HierarchicalClusteringJob implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private Integer levelsNb;
	private Integer clusterNb;
	
	@SuppressWarnings("unchecked")
	public List<JavaRDD<LabelPoint>> run(JavaSparkContext sc, JavaRDD<LabelPoint> clusterPoints) {
		ClusteringJob setClusterJob = new ClusteringJob();
		setClusterJob.setCluseterNb(clusterNb);
		List<JavaRDD<LabelPoint>> result = new ArrayList<>();
		
		if(levelsNb.equals(1))
			result.add(setClusterJob.run(clusterPoints));
		else {
			setClusterJob.run(clusterPoints)
						 .mapToPair((LabelPoint pt) -> new Tuple2<Integer, LabelPoint>(pt.getClusterPoint().getClusterId(clusterNb), pt))
						 .groupByKey()
						 .values()
						 .collect()
						 .forEach(values->{
							 List<LabelPoint> l = IteratorUtils.toList(values.iterator());
							 HierarchicalClusteringJob j = new HierarchicalClusteringJob();
							 j.setClusterNb(clusterNb);
							 j.setLevelsNb(levelsNb-1);
							 
							 result.addAll(j.run(sc, sc.parallelize(l)));
						 });
		}
		
		return result;
	}
	
	public void setLevelsNb(Integer levelsNb) {
		this.levelsNb = levelsNb;
	}
	
	public void setClusterNb(Integer clusterNb) {
		this.clusterNb = clusterNb;
	}
}
