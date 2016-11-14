package preprocess.pivotselection.kcover;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import util.kdtree.KDTree;

public class KCoverCentralizedWithKDTreeIndex {

	private static IMetricSpace metricSpace = null;
	private static IMetric metric = null;
	private static int dim = 2;
	private static int K = 500;
	private static double domainRange = 10000;

	public static void main(String[] args) throws IOException {
		long begin = System.currentTimeMillis();
		try {
			metricSpace = MetricSpaceUtility.getMetricSpace("vector");
			metric = MetricSpaceUtility.getMetric("L2Metric");
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		ArrayList<DataPoint> pointList = new ArrayList<DataPoint>();
		KDTree searchListTree = new KDTree(dim);
		// read in points
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("./samplePoints.csv"));
//			br = new BufferedReader(new FileReader("./0.data"));
			while ((sCurrentLine = br.readLine()) != null) {
				Object obj = metricSpace.readObject(sCurrentLine, dim);
				DataPoint currentPoint = new DataPoint(obj);
				PointCluster currentCluster = new PointCluster(currentPoint);
				currentPoint.setCluster(currentCluster);
				pointList.add(currentPoint);
				searchListTree.insert(((Record) currentPoint.getPointRecord()).getValueDouble(), currentPoint);
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
		int treeSize = searchListTree.getTreeSize();
		System.out.println("Tree Size: " + treeSize);

		if (pointList.size() <= K) {
			return;
		}
		double radius = Math.pow((Math.pow(domainRange, dim) / treeSize), 1.0 / dim) / 2;

		while (treeSize > K) {
			// for each point and radius r, count how many point is covered
			int maxNumCoverPoint = 0;
			ArrayList<Object> maxCoverDataPointInSearchingList = new ArrayList<Object>();
			int iStarPointIndex = -1;

			for (int i = 0; i < pointList.size(); i++) {
				double[] tempLowk = new double[dim];
				double[] tempUppk = new double[dim];
				double[] pointCoordinate = ((Record) pointList.get(i).getPointRecord()).getValueDouble();
				for (int j = 0; j < dim; j++) {
					tempLowk[j] = Math.max(0, pointCoordinate[j] - radius);
					tempUppk[j] = Math.min(domainRange, pointCoordinate[j] + radius);
				}
				Object[] tempCoverList = searchListTree.range(tempLowk, tempUppk);
				int countCover = Math.min(tempCoverList.length - 1, treeSize - K);
				if (countCover > maxNumCoverPoint) {
					maxNumCoverPoint = countCover;
					maxCoverDataPointInSearchingList.clear();
					maxCoverDataPointInSearchingList.addAll(Arrays.asList(tempCoverList));
					iStarPointIndex = i;
				}
			} // end for

			if (iStarPointIndex != -1) { // have clusters to merge
				// merge all clusters into one
				DataPoint newCenter = pointList.get(iStarPointIndex);
				HashMap<Integer, DataPoint> newPointList = new HashMap<Integer, DataPoint>();
				newPointList.putAll(newCenter.getCluster().getPointList());
				for (Object point : maxCoverDataPointInSearchingList) {
					newPointList.putAll(((DataPoint) point).getCluster().getPointList());
				}
				PointCluster newCluster = new PointCluster(newCenter, newPointList);
				// update all points in the cluster: point to the newcluster,
				// and remove points from the searchList
				newCenter.setCluster(newCluster);
				// System.out.println("Delete count: " + newPointList.size());
				for (DataPoint pointId : newPointList.values()) {
					pointId.setCluster(newCluster);
					try {
						searchListTree.delete(((Record) pointId.getPointRecord()).getValueDouble());
					} catch (RuntimeException e) {
					}
				}
				searchListTree.insert(((Record) newCenter.getPointRecord()).getValueDouble(), newCenter);
				treeSize = searchListTree.getTreeSize();
				// treeSize = searchListTree.range(lowk, uppk).length;
				 System.out.println(treeSize);
				radius = Math.pow((Math.pow(domainRange, dim) / treeSize), 1.0 / dim) / 2;
			} else
				radius = radius * 2;
			// break;
		}

		// output clusters
		int count = 0;
		double[] lowk = new double[dim];
		double[] uppk = new double[dim];
		for (int i = 0; i < dim; i++) {
			lowk[i] = 0;
			uppk[i] = domainRange;
		}
		Object[] finalResult = searchListTree.range(lowk, uppk);
		for (int i = 0; i < finalResult.length; i++) {
			count += ((DataPoint) finalResult[i]).getCluster().getPointList().size();
			System.out.println("Cluster " + i + "    num of points: "
					+ ((DataPoint) finalResult[i]).getCluster().getPointList().size());
		} // end for
		System.out.println("Total points: " + count);
		File file = new File("./FinalResult.txt");
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);
		
		System.out.println("File written Successfully");

		for (int i = 0; i < finalResult.length; i++) {
			System.out.println(((DataPoint) finalResult[i]).getCluster().getCentralPoint().getPointRecord().toString());
			bw.write(((DataPoint) finalResult[i]).getCluster().getCentralPoint().getPointRecord().toString());
			bw.newLine();
		} // end for
		bw.close();
		fw.close();
		long end = System.currentTimeMillis();
		System.out.println("Total Time: " + (end-begin)/1000);
	}

}
