package data;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class ClusterPoint implements Data<ClusterPoint>{

	private static final long serialVersionUID = 1L;
	
	private Point point;
	private Map<Integer, Integer> clusters;
	
	public ClusterPoint(Point point) {
		this.point = point.clone();
		this.clusters = new HashMap<>();
	}

	public Point getPoint() {
		return point;
	}

	public Map<Integer, Integer> getClusters() {
		return clusters;
	}

	public void addClusterId(Integer id) {
		clusters.put(clusters.size(), id);
	}
	
	public Integer getClusterId(int clusterNb) {
		Set<Integer> levels = clusters.keySet();
		Integer id = 0;
		
		for(Integer k:levels) {
			int x = clusters.get(k);
			id += x * (int)Math.pow(clusterNb, k);
		}
		
		return id;
	}
	
	public void setPoint(Point point) {
		this.point = point;
	}

	public void setClusterId(Integer level, Integer id) {
		this.clusters.put(level, id);
	}

	public void setClusters(Map<Integer, Integer> clusters) {
		this.clusters.clear();
		this.clusters.putAll(clusters);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((point == null) ? 0 : point.hashCode());
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
		ClusterPoint other = (ClusterPoint) obj;
		if (point == null) {
			if (other.point != null)
				return false;
		} else if (!point.equals(other.point))
			return false;
		return true;
	}

	@Override
	public ClusterPoint clone() {
		ClusterPoint clusterPoint = null;
		
		try {
			clusterPoint = (ClusterPoint) super.clone();
			clusterPoint.point = point.clone();
			
			clusterPoint.clusters = new HashMap<>();
			clusterPoint.clusters.putAll(clusters);
		}
		catch (CloneNotSupportedException e) {
		// Impossible
		e.printStackTrace();
		}
		
		return clusterPoint;
	}
	
	@Override
	public Double distance(ClusterPoint clusterPoint) {
		return point.distance(clusterPoint.point);
	}
	
	@Override
	public String toString() {
		StringJoiner str = new StringJoiner(",");
		Set<Integer> keys = clusters.keySet();
		
		for(Integer l:keys)
			str.add(String.format("%s:%s", l, clusters.get(l)));
		
		return String.format("%s {clusters: %s, %s}", ClusterPoint.class.getSimpleName(), str, point);
	}
	
	@Override
	public ClusterPoint add(ClusterPoint p) {
		ClusterPoint c = new ClusterPoint(this.point.add(p.point));
		c.clusters = this.clusters;
		
		return c;
	}
	
	@Override
	public ClusterPoint devide(Integer n) {
		ClusterPoint c= new ClusterPoint(this.point.devide(n));
		c.clusters.putAll(this.clusters);
		
		return c;
	}
	
	public Integer getBelowstCluster() {
		return clusters.get(clusters.size() - 1);
	}
	
	public void setBelowstCluster(Integer id) {
		clusters.put(clusters.size() - 1, id);
	}
	
	public void decrementClustersLevel() {
		clusters.remove(clusters.size()-1);
	}
}
