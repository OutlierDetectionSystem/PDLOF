package preprocess.pivotselection.kcenterNoOutlier;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import preprocess.pivotselection.kcover.DataPoint;
import preprocess.pivotselection.kcover.PointCluster;

class PointTuple {
	private Record point;
	private float distToPivot;
	private Record pivotPoint = null;

	public PointTuple(Record point, float distToPivot) {
		this.point = point;
		this.distToPivot = distToPivot;
	}

	public String getRecordString() {
		return point.toString();
	}

	public Record getPoint() {
		return point;
	}

	public float getDistToPivot() {
		return distToPivot;
	}

	public void setDistToPivot(float newDistToPivot) {
		this.distToPivot = newDistToPivot;
	}

	public void setPivotPoint(Record pivotPoint) {
		this.pivotPoint = pivotPoint;
	}

	public Record getPivotPoint() {
		return pivotPoint;
	}
}

public class KCenterCentralized {
	private static IMetricSpace metricSpace = null;
	private static IMetric metric = null;
	private static int dim = 2;
	private static int K = 100;
	private static double domainRange = 10000;

	public static void main(String[] args) throws IOException {
		try {
			metricSpace = MetricSpaceUtility.getMetricSpace("vector");
			metric = MetricSpaceUtility.getMetric("L2Metric");
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		int num_data = 0; // number of data
		ArrayList<PointTuple> allPoints = new ArrayList<PointTuple>();

		// read in points
		BufferedReader br = null;
		try {
			String line;
			br = new BufferedReader(new FileReader("./0.data"));
			while ((line = br.readLine()) != null) {
				num_data++;
				Object obj = metricSpace.readObject(line, dim);
				allPoints.add(new PointTuple((Record) obj, Float.POSITIVE_INFINITY));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		// begin selection
		Hashtable<Record, Integer> pivots = new Hashtable<Record, Integer>();
		Record newPivot = allPoints.get(0).getPoint();
		PointTuple newPointTuple = allPoints.get(0);
		// pivots.put(allPoints.get(0).getPoint(),num_data);
		allPoints.remove(0);

		/**
		 * when the number of pivots is less than K centers, greedy add pivots
		 * Assign all points to the nearest pivot and record the one that has
		 * the maximum distance to its pivot In the end, select the furthest one
		 * as the new pivot
		 */
		while (pivots.size() < K) {
			float maxDist = 0.0f;
			int indexOfNewCandidate = -1;
			int numOfPointsInNewPivot = 1;
			// first assign all other points to the new pivot, change if it
			// nears the new pivot
			// at the same time, track the maxDist
			for (int i = 0; i < allPoints.size(); i++) {
				Record tempPoint = allPoints.get(i).getPoint();
				float distToNewPivot = metric.dist(newPivot, tempPoint);
				if (distToNewPivot < allPoints.get(i).getDistToPivot()) {
					if (allPoints.get(i).getPivotPoint() != null) {
						// update the old pivot (reduce the number of points
						// assigned to that pivot)
						pivots.put(allPoints.get(i).getPivotPoint(), pivots.get(allPoints.get(i).getPivotPoint()) - 1);
					}
					allPoints.get(i).setDistToPivot(distToNewPivot);
					allPoints.get(i).setPivotPoint(newPivot);
					numOfPointsInNewPivot++;
				}
				if (allPoints.get(i).getDistToPivot() > maxDist) {
					maxDist = allPoints.get(i).getDistToPivot();
					indexOfNewCandidate = i;
				}
			} // end for
				// create the pivot
			pivots.put(newPivot, numOfPointsInNewPivot);
			// remove the new pivot from the original pivot pool
			if (newPointTuple.getPivotPoint() != null)
				pivots.put(newPointTuple.getPivotPoint(), pivots.get(newPointTuple.getPivotPoint()) - 1);
			// System.out.println("# of pivots = "+ pivots.size() + ", Max
			// kdistance = " + maxDist + ", numInNewPivot = " +
			// numOfPointsInNewPivot);

			// set the candidate to be the new pivot
			newPivot = allPoints.get(indexOfNewCandidate).getPoint();
			newPointTuple = allPoints.get(indexOfNewCandidate);
			allPoints.remove(indexOfNewCandidate);
		}
		// output all pivots
		Iterator iter = pivots.entrySet().iterator();
		int numOfPointsInside = 0;
		while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			Record keyHt = (Record) entry.getKey();
			int valueHt = (Integer) entry.getValue();
			numOfPointsInside += valueHt;
			System.out.println(keyHt.toString() + "," + valueHt);
		}
		// System.out.println("All Points in this partition: " +
		// numOfPointsInside);

	}
}
