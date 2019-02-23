package data;

public class AvgLabelPoint implements Data<AvgLabelPoint>{

	private static final long serialVersionUID = 1L;
	private LabelPoint sum;
	private Integer nb;
	
	public AvgLabelPoint(LabelPoint p) {
		sum = p.clone();
		nb = 1;
	}
	
	@Override
	public AvgLabelPoint add(AvgLabelPoint avg) {
		AvgLabelPoint res = new AvgLabelPoint(sum.add(avg.sum));
		res.nb = this.nb + avg.nb;
		
		return res;
	}
	
	public  LabelPoint moy() {
		LabelPoint c = sum.devide(nb);
		return c;
	}
	
	@Override 
	public AvgLabelPoint clone() {
		AvgLabelPoint avg = null;
		
		try {
			avg = (AvgLabelPoint) super.clone();
			avg.sum = sum.clone();			
		} catch (CloneNotSupportedException e) {
			// Impossible
			e.printStackTrace();
		}
		
		return avg;
	}
	
	@Override
	public String toString() {
		return String.format("%s{sum: %s, nb: %s}", AvgLabelPoint.class.getSimpleName(), sum, nb);
	}

	@Override
	public AvgLabelPoint devide(Integer n) {
		AvgLabelPoint avg = new AvgLabelPoint(sum.devide(n));
		avg.nb = this.nb;
		
		return avg;
	}

	@Override
	public Double distance(AvgLabelPoint data) {
		return sum.distance(data.sum);
	}

	public LabelPoint getSum() {
		return sum;
	}

	public void setSum(LabelPoint sum) {
		this.sum = sum.clone();
	}
}
