import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import data.LabelPoint;

public class InitialisationJob implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private int columns[];
	private String sep;
	private int labelColumn;
	private int meteringColumn;
	
	public Function<String, LabelPoint> parsePoint = s -> {
		LabelPoint pt = null;
		
		try {
			pt = LabelPoint.parseLabelPoint(s, columns, labelColumn, meteringColumn, sep);
		}
		catch(Exception ex) {
		}
		
		return pt;
	};
	
	public JavaRDD<LabelPoint> run(JavaSparkContext sc, String inputPath) {

		
		JavaRDD<LabelPoint> points = sc.textFile(inputPath)
				  .map(line -> parsePoint.call(line))
				  .filter((LabelPoint pt) -> pt != null)
				  .distinct();
		
		return points;
	}
	
	public void setColumns(int[] columns) {
		this.columns = columns;
	}
	
	public void setSep(String sep) {
		this.sep = sep;
	}
	
	public void setMeteringColumn(int meteringColumn) {
		this.meteringColumn = meteringColumn;
	}
	
	public void setLabelColumn(int labelColumn) {
		this.labelColumn = labelColumn;
	}
}
