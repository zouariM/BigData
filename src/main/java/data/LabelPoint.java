package data;

public class LabelPoint implements Data<LabelPoint>, Comparable<LabelPoint>{
	
	private static final long serialVersionUID = 1L;
	private ClusterPoint clusterPoint;
	private String label;
	private Double metering;
	
	public LabelPoint(ClusterPoint clusterPoint) {
		this.clusterPoint = clusterPoint.clone();
	}
	
	@Override
	public LabelPoint add(LabelPoint data) {
		return new LabelPoint(clusterPoint.add(data.clusterPoint));
	}

	@Override
	public LabelPoint devide(Integer n) {
		return new LabelPoint(clusterPoint.devide(n));
	}

	@Override
	public Double distance(LabelPoint data) {
		return clusterPoint.distance(data.clusterPoint);
	}

	public ClusterPoint getClusterPoint() {
		return clusterPoint;
	}

	public String getLabel() {
		return label;
	}

	public Double getMetering() {
		return metering;
	}

	public void setClusterPoint(ClusterPoint clusterPoint) {
		this.clusterPoint = clusterPoint.clone();
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public void setMetering(Double metering) {
		this.metering = metering;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clusterPoint == null) ? 0 : clusterPoint.hashCode());
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
		LabelPoint other = (LabelPoint) obj;
		if (clusterPoint == null) {
			if (other.clusterPoint != null)
				return false;
		} else if (!clusterPoint.equals(other.clusterPoint))
			return false;
		return true;
	}

	@Override
	public LabelPoint clone() {
		LabelPoint labelPoint = null;
		
		try {
			labelPoint = (LabelPoint) super.clone();
			labelPoint.clusterPoint = clusterPoint.clone();
		}
		catch (CloneNotSupportedException e) {
		// Impossible
		e.printStackTrace();
		}
		
		return labelPoint;
	}

	@Override
	public int compareTo(LabelPoint o) {
		return this.getMetering().compareTo(o.getMetering());
	}

	public static LabelPoint parseLabelPoint(String line, int columns[], int labelColumn, 
											int meteringColumn, String sep) 
	{
		ClusterPoint clusterPoint = new ClusterPoint(Point.parseLine(line, columns, sep));
		String fields[] = line.split(sep);
		
		LabelPoint labelPoint = new LabelPoint(clusterPoint);
		labelPoint.label = fields[labelColumn];
		labelPoint.metering = Double.parseDouble(fields[meteringColumn]);
		
		return labelPoint;
	}
	
	@Override
	public String toString() {
		return String.format("%s {%s, label: %s, metering: %s}", LabelPoint.class.getSimpleName(),
								clusterPoint, label, metering);
	}
}
