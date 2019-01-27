package data;
import java.util.Arrays;
import java.util.StringJoiner;

public class Point implements Data<Point>{
	private static final long serialVersionUID = 1L;
	
	private Double coordinates[];
	
	public Point(Double coordinates[]) {
		this.coordinates = coordinates.clone();
	}
	
	public static Point parseLine(String line, int columns[], String sep) {
		String fields[] = line.split(sep);
		Double coordinates[] = new Double[columns.length];
		
		for(int i=0; i<columns.length; i++)
			coordinates[i] = Double.parseDouble(fields[columns[i]]);
		
		return new Point(coordinates);
	}

	public Double[] getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(Double[] coordinates) {
		this.coordinates = coordinates;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(coordinates);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Point other = (Point) obj;
		if (!Arrays.equals(coordinates, other.coordinates))
			return false;
	
		return true;
	}
	
	@Override
	public Point clone() {
		Point point = null;
		
		try {
			point = (Point) super.clone();
			point.coordinates = coordinates.clone();			
		} catch (CloneNotSupportedException e) {
			// Impossible
			e.printStackTrace();
		}
		
		return point;
	}
	
	@Override
	public String toString() {
		StringJoiner str = new StringJoiner(",");
		for(Double d:coordinates)
			str.add(d.toString());
		
		return String.format("%s {%s}", Point.class.getSimpleName(), str);
	}
	
	@Override
	public Point add(Point pt) {
		Double coordinates[] = new Double[this.coordinates.length];
		
		for(int i=0; i<coordinates.length; i++)
			coordinates[i] = this.coordinates[i] + pt.coordinates[i];
		
		return new Point(coordinates);
	}
	
	@Override
	public Point devide(Integer n) {
		Double coordinates[] = new Double[this.coordinates.length];
		
		for(int i=0; i<coordinates.length; i++)
			coordinates[i] = this.coordinates[i]/n;
		
		return new Point(coordinates);
	}

	@Override
	public Double distance(Point data) {
		double d = 0d;
		
		for(int i=0; i<coordinates.length; i++)
			d += Math.pow(coordinates[i] - data.coordinates[i], 2d);
		
		return Math.sqrt(d);
	}
	
}
