import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import data.LabelPoint;

public class Main {
	
	public static final String SEP =",";
	
	public static void main(String[] args) {
		if(args.length < 7)
			throw new IllegalArgumentException("use <inputPath> <outputPath>"
					+ "<clusterNb> <levelsNb> <meteringColumn> <labelColumn>"
					+"<columns>...");
		
		String inputPath = args[0];
		@SuppressWarnings("unused")
		String outputPath = args[1];
		
		Integer clusterNb = Integer.parseInt(args[2]);
		Integer levelsNb = Integer.parseInt(args[3]);
		
		Integer meteringColumn = Integer.parseInt(args[4]);
		Integer labelColumn = Integer.parseInt(args[5]);
		
		int columns[] = new int[args.length-6];
		for(int i=0; i<columns.length; i++)
			columns[i] = Integer.parseInt(args[i+6]);
		
		SparkConf conf = new SparkConf().setAppName("Big Data Porject").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Parser points
		InitialisationJob initJob = new InitialisationJob();
		initJob.setColumns(columns);
		initJob.setSep(SEP);
		initJob.setLabelColumn(labelColumn);
		initJob.setMeteringColumn(meteringColumn);
		
		JavaRDD<LabelPoint> points = initJob.run(sc, inputPath).cache();
	
		//Hierarchical clustering
		HierarchicalClusteringJob hierarchicalClusteringJob = new HierarchicalClusteringJob();
		hierarchicalClusteringJob.setClusterNb(clusterNb);
		hierarchicalClusteringJob.setLevelsNb(levelsNb);
		
		List<JavaRDD<LabelPoint>> clusters = hierarchicalClusteringJob.run(sc, points);
		System.out.println("result size = " + clusters.size());
		
		//Labeling Job
		LabelingJob labelingJob = new LabelingJob();
		labelingJob.setClusterNb(clusterNb);
		labelingJob.setLevels(levelsNb);
		
		//ToDo put to HDFS file
		labelingJob.run(sc, clusters).forEach(s->{
			s.collect().forEach(System.out::println);
		});
		
		sc.close();
	}
	
}
