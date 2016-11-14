package preprocess.pivotselection.kcover;

import java.util.ArrayList;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.Record;

public class CellStore {
	public int cellStoreId;
	public Record coordinate;
	public int numberOfPoints;
	public ArrayList<Record> outputList;

	public CellStore(int cellStoreId, int dim) {
		this.cellStoreId = cellStoreId;
		outputList = new ArrayList<Record>();
		numberOfPoints = 0;
	}

	public boolean addPoint(Record newPoint, int maxRepresentative, int dim, IMetricSpace metricSpace) {
		if (newPoint.getValue().length != dim)
			return false;
		if (numberOfPoints >= maxRepresentative) {
			outputList.add(coordinate);
			coordinate = newPoint;
			numberOfPoints = 1;
		} else if (numberOfPoints == 0) {
			coordinate = newPoint;
			numberOfPoints++;
		} else {// recompute the coordinate
			String tempCompute = newPoint.getRId()+"";
			float [] CoorForNew = newPoint.getValue();
			float [] CoorForOld = coordinate.getValue();
			for (int i = 0; i < CoorForNew.length; i++) {
				tempCompute  += "," + (CoorForOld[i] * numberOfPoints + CoorForNew[i]) / (numberOfPoints + 1);
			}
			Record newCoordinate = (Record) metricSpace.readObject(tempCompute, dim);
			coordinate = newCoordinate;
			numberOfPoints++;
		}
		return true;
	}
	public CellStore() {

	}

	public String printCellStoreBasic() {
		String str = "";
		str += cellStoreId + ",";
		str += numberOfPoints;
		return str;
	}

	public static int ComputeCellStoreId(float[] data, int dim, int numCellPerDim, int smallRange) {
		if (data.length != dim)
			return -1;
		int cellId = 0;
		for (int i = 0; i < dim; i++) {
			int tempIndex = (int) Math.floor(data[i] / smallRange);
			cellId = cellId + (int) (tempIndex * Math.pow(numCellPerDim, i));
		}
		return cellId;
	}
}