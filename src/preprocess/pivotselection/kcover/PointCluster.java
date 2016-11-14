package preprocess.pivotselection.kcover;

import java.util.ArrayList;
import java.util.HashMap;

import metricspace.Record;

public class PointCluster {
	private DataPoint centralPoint;
	private HashMap<Integer, DataPoint> pointList;

	public PointCluster(DataPoint dp) {
		this.centralPoint = dp;
		pointList = new HashMap<Integer, DataPoint>();
		pointList.put(((Record) dp.getPointRecord()).getRId(), dp);
	}

	public PointCluster(DataPoint dp, HashMap<Integer, DataPoint> pointList) {
		this.centralPoint = dp;
		this.pointList = pointList;
	}

	public DataPoint getCentralPoint() {
		return centralPoint;
	}

	public void setCentralPoint(DataPoint centralPoint) {
		this.centralPoint = centralPoint;
	}

	public HashMap<Integer, DataPoint> getPointList() {
		return pointList;
	}

	public void setPointList(HashMap<Integer, DataPoint> pointList) {
		this.pointList = pointList;
	}
}
