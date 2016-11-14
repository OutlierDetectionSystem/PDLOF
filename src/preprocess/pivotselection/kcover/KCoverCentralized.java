package preprocess.pivotselection.kcover;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricSpaceUtility;
import metricspace.Record;

public class KCoverCentralized {

	private static IMetricSpace metricSpace = null;
	private static IMetric metric = null;
	private static int dim = 2;
	private static int K = 3;
	private static double domainRange = 250;

	public static void main(String[] args) {
		try {
			metricSpace = MetricSpaceUtility.getMetricSpace("vector");
			metric = MetricSpaceUtility.getMetric("L2Metric");
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		ArrayList<DataPoint> pointList = new ArrayList<DataPoint>();
		HashMap<Integer, DataPoint> searchList = new HashMap<Integer, DataPoint>();

		// read in points
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("./dataForPic_id.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				Object obj = metricSpace.readObject(sCurrentLine, dim);
				DataPoint currentPoint = new DataPoint(obj);
				PointCluster currentCluster = new PointCluster(currentPoint);
				currentPoint.setCluster(currentCluster);
				pointList.add(currentPoint);
				searchList.put(((Record) currentPoint.getPointRecord()).getRId(), currentPoint);
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
		System.out.println("Point size: " + searchList.size());

		if (pointList.size() <= K) {
			return;
		}
		double radius = Math.pow((Math.pow(domainRange, dim) / searchList.size()), 1.0 / dim)/10;
		while (searchList.size() > K) {
			System.out.println("Radius: " + radius + ", size :" + searchList.size());
			// for each point and radius r, count how many point is covered
			int maxNumCoverPoint = 0;
			ArrayList<Integer> maxCoverIndexInSearchingList = new ArrayList<Integer>();
			int iStarPointIndex = -1;

			for (int i = 0; i < pointList.size(); i++) {
				ArrayList<Integer> tempCoverIndexInSearching = new ArrayList<Integer>();
				for (Map.Entry<Integer, DataPoint> entry : searchList.entrySet()) {
					try {
						if (metric.dist(pointList.get(i).getPointRecord(),
								entry.getValue().getPointRecord()) < radius) {
							tempCoverIndexInSearching.add(entry.getKey());
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} // end for
					// int countCover = 0;
					// if(searchList.containsKey(((Record)
					// pointList.get(i).getPointRecord()).getRId()))
					// countCover = tempWeights - pointList.get(i).getWeight();
					// else
					// countCover = tempWeights;
				int countCover = Math.min(tempCoverIndexInSearching.size() - 1, searchList.size() - K);
				if (countCover > maxNumCoverPoint) {
					maxNumCoverPoint = countCover;
					maxCoverIndexInSearchingList.clear();
					maxCoverIndexInSearchingList.addAll(tempCoverIndexInSearching);
					iStarPointIndex = i;
				}
			} // end for
			if (iStarPointIndex != -1) { // have clusters to merge
				// merge all clusters into one
				DataPoint newCenter = pointList.get(iStarPointIndex);
				HashMap<Integer, DataPoint> newPointList = new HashMap<Integer, DataPoint>();
				newPointList.putAll(newCenter.getCluster().getPointList());
				for (Integer pointId : maxCoverIndexInSearchingList) {
					// if (pointId.intValue() != ((Record)
					// newCenter.getPointRecord()).getRId()) {
					newPointList.putAll(searchList.get(pointId).getCluster().getPointList());
					// }
				}
				PointCluster newCluster = new PointCluster(newCenter, newPointList);
				// update all points in the cluster: point to the newcluster,
				// and remove points from the searchList
				newCenter.setCluster(newCluster);
				for (DataPoint pointId : newPointList.values()) {
					pointId.setCluster(newCluster);
					searchList.remove(((Record) pointId.getPointRecord()).getRId());
				}
				// for (Integer pointId : maxCoverIndexInSearchingList) {
				// // searchList.get(pointId).setCluster(newCluster);
				// searchList.remove(pointId);
				// }
				searchList.put(((Record) newCenter.getPointRecord()).getRId(), newCenter);

				radius = Math.pow((Math.pow(domainRange, dim) / searchList.size()), 1.0 / dim)/10;
			} else
				radius = radius * 2;
		}

		// output clusters
		int count = 0;
		for (Map.Entry<Integer, DataPoint> entry : searchList.entrySet()) {
			count += entry.getValue().getCluster().getPointList().size();
			System.out.println(
					"Cluster " + count + "    num of points: " + entry.getValue().getCluster().getPointList().size());
		} // end for
		System.out.println("Total points: " + count);
		for (Map.Entry<Integer, DataPoint> entry : searchList.entrySet()) {
			System.out.println(entry.getValue().getCluster().getCentralPoint().getPointRecord().toString());
		} // end for
	}

}
