package dataspliting;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
//import java.util.HashMap;
//import java.util.Map;
import java.util.PriorityQueue;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import dataspliting.DataSplit.DataSplitMapper.ASCPQ;
import lof.baseline.Partition;
import preprocess.pivotselection.SQConfig;

public class IndexUtility {

	public IndexUtility() {
	}

	/**
	 * read pivots from a single file
	 * 
	 * @param pivotFile
	 * @return
	 * @throws IOException
	 */
	public Vector<Object> readPivotFromFile(String pivotFile, IMetricSpace metricSpace, int dim,
			org.apache.hadoop.conf.Configuration conf) throws IOException {
		Vector<Object> pivots = new Vector<Object>();
		FileSystem fs = FileSystem.get(conf);
		BufferedReader fis = null;
		FSDataInputStream in = fs.open(new Path(pivotFile));
		try {
			fis = new BufferedReader(new InputStreamReader(in));
			String line;
			while ((line = fis.readLine()) != null) {
//				System.out.println("line" + line);
				pivots.add(metricSpace.readObject(line, dim));
			}
			return pivots;
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '" + pivotFile + "'");
			return null;
		} finally {
			if (in != null) {
				fis.close();
				in.close();
			}
		}
	}

	/**
	 * read pivots from a single file
	 * 
	 * @param pivotFile
	 * @return
	 * @throws IOException
	 */
	public Vector<Object> readQueryFromFile(String queryFile, IMetricSpace metricSpace, int dim,
			org.apache.hadoop.conf.Configuration conf) throws IOException {
		return readPivotFromFile(queryFile, metricSpace, dim, conf);
	}

	public Vector<Partition> readIndexFromFile(String indexFile, Configuration conf) throws IOException {

		Vector<Partition> partSet = new Vector<Partition>();
		FileSystem fs = FileSystem.get(conf);
		BufferedReader fis = null;
		FSDataInputStream in = fs.open(new Path(indexFile));
		try {
			String line;
			String[] info;
			int i;
			int total_sum = 0;
			fis = new BufferedReader(new InputStreamReader(in));
			while ((line = fis.readLine()) != null) {
				info = line.split(SQConfig.sepStrForIndex);
				/**
				 * 0: pid; 1: min_radius; 2: max_radius; 3: size; 4:num; 5
				 * <dist>
				 */
				Partition part = new Partition();
				part.setMinMaxRadius(Float.valueOf(info[1]), Float.valueOf(info[2]));
				part.setSizeOfPartition(Integer.valueOf(info[3]));
				part.setNumOfObjects(Integer.valueOf(info[4]));
				total_sum += Integer.valueOf(info[4]);
				Vector<Float> distSet = new Vector<Float>();
				for (i = info.length - 1; i > 4; i--) {
					distSet.add(Float.valueOf(info[i]));
				}
				part.setDistOfFirstKObjects(distSet);
				partSet.add(part);
			}

//			System.out.println("total_sum: " + total_sum);
			return partSet;
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '" + indexFile + "'");
			return null;
		} finally {
			if (fis != null) {
				in.close();
				fis.close();
			}
		}
	}

	public Vector<Integer>[] readGroupsFromFile(String indexFile, int numOfGroups) throws IOException {
		@SuppressWarnings("unchecked")
		Vector<Integer>[] groups = new Vector[numOfGroups];
		BufferedReader fis = null;
		try {
			String line;
			String[] info;
			int i = 0, j;
			fis = new BufferedReader(new FileReader(indexFile));
			while ((line = fis.readLine()) != null) {
				info = line.split(SQConfig.sepSplitForGroupIDs);
				groups[i] = new Vector<Integer>();
				for (j = 0; j < info.length; j++) {
					groups[i].add(Integer.valueOf(info[j]));
				}
				i++;
			}
			return groups;
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '" + indexFile + "'");
			return null;
		} finally {
			if (fis != null) {
				fis.close();
			}
		}
	}

	public float[][] readLMatrixFromFile(String filename, int numOfPivots) throws IOException {
		DataInputStream fis = null;
		float[][] lMatrix = new float[numOfPivots][numOfPivots];
		try {
			int i, j;
			fis = new DataInputStream(new FileInputStream(filename));
			for (i = 0; i < numOfPivots; i++) {
				for (j = 0; j < numOfPivots; j++) {
					lMatrix[i][j] = fis.readFloat();
				}
			}
			return lMatrix;
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file '" + filename + "'");
			return null;
		} finally {
			if (fis != null) {
				fis.close();
			}
		}
	}

	/**
	 * get upper bound for each partition in pSet Assumption:
	 * 
	 * @param pSet
	 *            : pivot, min_r, max_r, dist1, dist2, ....,
	 * @return
	 */
	public float[] getUpperBound(Vector<Partition> pSet, float[][] distMatrix, int K) throws IOException {
		int numOfPart = pSet.size();
		float[] ub = new float[numOfPart];
		int i, j;

		float dist;
		// float tmp;
		for (i = 0; i < numOfPart; i++) {
			Partition p1 = pSet.get(i);

			PriorityQueue<Float> knn = new PriorityQueue<Float>(K, new ASCPQ<Float>());
			for (j = 0; j < numOfPart; j++) {
				if (i == j) {
//					continue;
					for (float d : p1.getDistOfFirstKObjects()) {
						float max_dist = d  + p1.getMaxRadius();
						if (knn.size() < K) {
							knn.add(max_dist);
						} else if (max_dist < knn.peek()) {
							knn.remove();
							knn.add(max_dist);
						} else {
							break;
						}
					}
				}
				Partition p2 = pSet.get(j);
				dist = distMatrix[i][j];
				for (float d : p2.getDistOfFirstKObjects()) {
					float max_dist = d + dist + p1.getMaxRadius();
					;
					if (knn.size() < K) {
						knn.add(max_dist);
					} else if (max_dist < knn.peek()) {
						knn.remove();
						knn.add(max_dist);
					} else {
						break;
					}
				}
			}
			ub[i] = knn.peek();
		}
		return ub;
	}

	/**
	 * get upper bound for each partition in pSet Assumption:
	 * 
	 * @param pSet
	 *            : pivot, min_r, max_r, dist1, dist2, ....,
	 * @return
	 */
	public float[] getUpperBoundInside(Vector<Partition> pSet, float[][] distMatrix, int K) throws IOException {
		int numOfPart = pSet.size();
		float[] ub = new float[numOfPart];
		int i, j;

		// float tmp;
		for (i = 0; i < numOfPart; i++) {
			Partition p1 = pSet.get(i);

			PriorityQueue<Float> knn = new PriorityQueue<Float>(K, new ASCPQ<Float>());

			for (float d : p1.getDistOfFirstKObjects()) {
				float max_dist = d + p1.getMaxRadius();
				if (knn.size() < K) {
					knn.add(max_dist);
				} else if (max_dist < knn.peek()) {
					knn.remove();
					knn.add(max_dist);
				}
			}
			ub[i] = knn.peek();
		}
		return ub;
	}
	/**
	 * get upper bound for each partition in pSet Assumption:
	 * 
	 * @param pSet
	 *            : pivot, min_r, max_r, dist1, dist2, ....,
	 * @return
	 */
	public float[] getUpperBoundNoDistMatrix(Vector<Partition> pSet, Vector<Object>pivots, int K, IMetric metric) throws IOException {
		int numOfPart = pSet.size();
		float[] ub = new float[numOfPart];
		int i, j;

		float dist;
		// float tmp;
		for (i = 0; i < numOfPart; i++) {
			Partition p1 = pSet.get(i);

			PriorityQueue<Float> knn = new PriorityQueue<Float>(K, new ASCPQ<Float>());
			for (j = 0; j < numOfPart; j++) {
				if (i == j) {
//					continue;
					for (float d : p1.getDistOfFirstKObjects()) {
						float max_dist = d  + p1.getMaxRadius();
						if (knn.size() < K) {
							knn.add(max_dist);
						} else if (max_dist < knn.peek()) {
							knn.remove();
							knn.add(max_dist);
						} else {
							break;
						}
					}
				}
				Partition p2 = pSet.get(j);
				dist = metric.dist(pivots.get(i),pivots.get(j));
				for (float d : p2.getDistOfFirstKObjects()) {
					float max_dist = d + dist + p1.getMaxRadius();
					;
					if (knn.size() < K) {
						knn.add(max_dist);
					} else if (max_dist < knn.peek()) {
						knn.remove();
						knn.add(max_dist);
					} else {
						break;
					}
				}
			}
			ub[i] = knn.peek();
		}
		return ub;
	}
	
}
