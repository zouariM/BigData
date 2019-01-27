import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import data.AvgLabelPoint;
import data.ClusterPoint;
import data.LabelPoint;
import scala.Tuple2;

public class ClusteringJob implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private Integer cluseterNb;
	private List<ClusterPoint> oldCentroids = new ArrayList<>();
	
	public PairFunction<LabelPoint, Integer, AvgLabelPoint> clusterPair= (LabelPoint pt)->{
		ClusterPoint min = oldCentroids.parallelStream().min((ClusterPoint c1, ClusterPoint c2)->{
			Double d1 = new Double(pt.getClusterPoint().distance(c1));
			Double d2 = new Double(pt.getClusterPoint().distance(c2));
			
			return d1.compareTo(d2);
		}).get();
		
		Integer key = min.getBelowstCluster();
		LabelPoint c = pt.clone();
		c.getClusterPoint().setClusters(min.getClusters());
		AvgLabelPoint value = new AvgLabelPoint(c);
		
		return new Tuple2<Integer, AvgLabelPoint>(key, value);
	};

	public Function<Iterable<AvgLabelPoint>, ClusterPoint> getCentroid = values ->{
		Iterator<AvgLabelPoint> it = values.iterator();
		AvgLabelPoint avg = it.next().clone();
		
		while(it.hasNext())
			avg.add(it.next());
		
		return avg.moy().getClusterPoint();
	};
	
	public JavaRDD<LabelPoint> run(JavaRDD<LabelPoint> points){
		initCentroids(points);
		JavaPairRDD<Integer, AvgLabelPoint> avgPoints;
		
		do {
			avgPoints = points.mapToPair((LabelPoint p) -> clusterPair.call(p))
							  .cache();
			List<ClusterPoint> newCentroids = avgPoints.reduceByKey(AvgLabelPoint::add)
												.groupByKey()
												.values()
												.map((Iterable<AvgLabelPoint> values) -> getCentroid.call(values))
												.collect();
		
			System.out.println("new");
			newCentroids.forEach(System.out::println);
			System.out.println("old");
			oldCentroids.forEach(System.out::println);
			
			if(ListUtils.isEqualList(oldCentroids, newCentroids))
				break;
			else {
				oldCentroids.clear();
				oldCentroids.addAll(newCentroids);
				
				avgPoints.unpersist();
			}
		}while(true);
		
		
		return avgPoints.values().map((AvgLabelPoint avg) -> avg.getSum());
	}

	public void setCluseterNb(Integer cluseterNb) {
		this.cluseterNb = cluseterNb;
	}

	private void initCentroids(JavaRDD<LabelPoint> points) {
		oldCentroids.clear();
		oldCentroids.addAll(points.take(cluseterNb)
							 .stream()
							 .map((LabelPoint pt)->pt.getClusterPoint())
							 .collect(Collectors.toList())
							);
		
		int i=0;
		for(ClusterPoint c:oldCentroids)
			c.addClusterId(i++);	
	}
}
