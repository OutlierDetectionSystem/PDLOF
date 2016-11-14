package preprocess.pivotselection.kcover;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import util.kdtree.*;

public class KDTreeTest {

	private static IMetricSpace metricSpace = null;
	private static int dim = 2;

	private static double domainRange = 10000;

	public static void main(String[] args) {
		try {
			metricSpace = MetricSpaceUtility.getMetricSpace("vector");
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		ArrayList<DataPoint> pointList = new ArrayList<DataPoint>();
		KDTree searchListTree = new KDTree(dim);

		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("./test.data"));
			while ((sCurrentLine = br.readLine()) != null) {
				Object obj = metricSpace.readObject(sCurrentLine, dim);
				DataPoint currentPoint = new DataPoint(obj);
				PointCluster currentCluster = new PointCluster(currentPoint);
				currentPoint.setCluster(currentCluster);
				pointList.add(currentPoint);
				searchListTree.insert(((Record) currentPoint.getPointRecord()).getValueDouble(), currentPoint);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		double[] lowk = new double[dim];
		double[] uppk = new double[dim];
		for (int i = 0; i < dim; i++) {
			lowk[i] = 0;
			uppk[i] = domainRange;
		}

		int treeSize = 0;
		try {
			treeSize = searchListTree.range(lowk, uppk).length;
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Tree Size: " + treeSize);

		System.out.println("deleting: " + ((Record) pointList.get(0).getPointRecord()).getValueDouble().toString());
		try {
			searchListTree.delete(((Record) pointList.get(0).getPointRecord()).getValueDouble());
			searchListTree.delete(((Record) pointList.get(0).getPointRecord()).getValueDouble());
		} catch (RuntimeException e) {
			
		}
		try {
			treeSize = searchListTree.range(lowk, uppk).length;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Tree Size: " + searchListTree.getTreeSize());
		searchListTree.insert(((Record) pointList.get(0).getPointRecord()).getValueDouble(), pointList.get(0));
		searchListTree.insert(((Record) pointList.get(0).getPointRecord()).getValueDouble(), pointList.get(0));
		try {
			treeSize = searchListTree.range(lowk, uppk).length;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Tree Size: " + searchListTree.getTreeSize());
	}

}
