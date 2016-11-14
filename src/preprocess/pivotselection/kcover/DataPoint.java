package preprocess.pivotselection.kcover;

public class DataPoint {

	private Object pointRecord;
	private PointCluster cluster;
	
	public DataPoint(Object currentPoint){
		this.pointRecord = currentPoint;
	}
	public Object getPointRecord() {
		return pointRecord;
	}

	public void setPointRecord(Object pointRecord) {
		this.pointRecord = pointRecord;
	}
	public PointCluster getCluster() {
		return cluster;
	}
	public void setCluster(PointCluster cluster) {
		this.cluster = cluster;
	}
}
