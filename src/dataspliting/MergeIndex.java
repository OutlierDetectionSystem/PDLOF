package dataspliting;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import preprocess.pivotselection.SQConfig;

/**
 * 
 * derive the summary from all reducer for each partition
 * 
 * @author Yizhou Yan, modified luwei
 *
 */
public class MergeIndex {
	int numOfPivots;
	int K;
	private float[] min_R, max_R;
	private int[] numOfObjects_R, sizeOfObjects_R, numOfObjectsLarger11G;
	private PriorityQueue<Float>[] KNNObjectsToPivots_S;

	public static class ASCPQ<T> implements Comparator<T> {
		public int compare(Object o1, Object o2) {
			float v1 = (Float) o1;
			float v2 = (Float) o2;

			if (v1 > v2)
				return -1;
			else if (v1 == v2)
				return 0;
			else
				return 1;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MergeIndex(int numOfPivots, int K) {
		this.numOfPivots = numOfPivots;
		this.K = K;
		min_R = new float[numOfPivots];
		max_R = new float[numOfPivots];
		numOfObjects_R = new int[numOfPivots];
		sizeOfObjects_R = new int[numOfPivots];
		numOfObjectsLarger11G = new int[numOfPivots];
		KNNObjectsToPivots_S = new PriorityQueue[numOfPivots];
		for (int i = 0; i < numOfPivots; i++) {
			min_R[i] = Float.MAX_VALUE;
			max_R[i] = 0;
			numOfObjects_R[i] = 0;
			sizeOfObjects_R[i]  = 0;
			numOfObjectsLarger11G[i] = 0;
			KNNObjectsToPivots_S[i] = new PriorityQueue<Float>(K, new ASCPQ());
		}
	}

	public void mergeAll(String input_dir, String output) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(input_dir));

		for (int i = 0; i < stats.length; ++i) {
			if (!stats[i].isDirectory()) {
				/** parse file */
				parseFile(fs, stats[i].getPath().toString());
			}
		}
		writeToHDFS(fs, output);
		fs.close();
	}

	/**
	 * format of each line:  pid, min_r, max_r, size, number, kNN dist list 
	 * 
	 * @param fs
	 * @param filename
	 */
	public void parseFile(FileSystem fs, String filename) {
		try {
			// check if the file exists
			Path path = new Path(filename);
			if (fs.exists(path)) {
				FSDataInputStream currentStream;
				BufferedReader currentReader;
				currentStream = fs.open(path);
				currentReader = new BufferedReader(new InputStreamReader(
						currentStream));
				String line;
				int i;
				while ((line = currentReader.readLine()) != null) {
					/** parse line */
					String[] values = line.split(SQConfig.sepStrForIndex);
					int p_id = Integer.valueOf(values[0]);
					float min_r = Float.valueOf(values[1]);
					float max_r = Float.valueOf(values[2]);
					int size = Integer.valueOf(values[3]);
					int num = Integer.valueOf(values[4]);
					int numForG = Integer.valueOf(values[5]);
						/** R */
						if (min_r < min_R[p_id])
							min_R[p_id] = min_r;
						if (max_r > max_R[p_id])
							max_R[p_id] = max_r;
						numOfObjects_R[p_id] += num;
						sizeOfObjects_R[p_id] += size;
						numOfObjectsLarger11G[p_id] += numForG;
						/** update the Priority Queue */
						for (i = 6; i < values.length; i++) {
							float dist = Float.valueOf(values[i]);
							if (KNNObjectsToPivots_S[p_id].size() < K) {
								KNNObjectsToPivots_S[p_id].add(dist);
							} else {
								if (KNNObjectsToPivots_S[p_id].peek() > dist) {
									KNNObjectsToPivots_S[p_id].remove();
									KNNObjectsToPivots_S[p_id].add(dist);
								}
							}
						}
				}
				currentReader.close();
			} else {
				throw new Exception("the file is not found .");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void writeToHDFS(FileSystem fs, String output_dir) {
		double sumOfDiameter = 0;
		try {
			String filename = output_dir + "/summary"
					+ SQConfig.strIndexExpression1;
			Path path = new Path(filename);
			System.out.println("output path:"+ path);
			FSDataOutputStream currentStream;
			currentStream = fs.create(path, true);
			String line;
			int total_num = 0;
			int countLessThanKPoints = 0;
			int countLarger11G = 0;
			int countPointsLarger11G = 0;
			/** writhe the summary information */
			for (int i = 0; i < numOfPivots; i++) {
				line = Integer.toString(i) + SQConfig.sepStrForIndex
						+ min_R[i] + SQConfig.sepStrForIndex + max_R[i]
						+ SQConfig.sepStrForIndex + sizeOfObjects_R[i]
						+ SQConfig.sepStrForIndex + numOfObjects_R[i]
								+SQConfig.sepStrForIndex + numOfObjectsLarger11G[i];
				sumOfDiameter += max_R[i];
				/** output larger than G and number less than K*/
				if(KNNObjectsToPivots_S[i].size() < K){
//					System.out.println("Less than K points: " + line);
					countLessThanKPoints++;
				}
				if(numOfObjectsLarger11G[i] >0){
//					System.out.println("Larger 11G: " + line);
					countLarger11G++;
					countPointsLarger11G += numOfObjectsLarger11G[i];
				}
				PriorityQueue<Float> pq = KNNObjectsToPivots_S[i];
				while (pq.size() > 0) {
					line += SQConfig.sepStrForIndex + pq.remove();
				}
				line += "\n";
				currentStream.writeBytes(line);
				
				total_num += numOfObjects_R[i];
			}
			currentStream.close();
			System.out.println("numOfObjects" + total_num);
			System.out.println("# of pivots Less than k points: "+ countLessThanKPoints);
			System.out.println("# of pivots Larger than 11G: " + countLarger11G);
			System.out.println("# of points Larger than 11G:" + countPointsLarger11G );
			System.out.println("Sum of diameter: " +sumOfDiameter);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		try {
			// String input = args[0];
			String input = conf.get(SQConfig.strIndexOutput);
			String output = conf.get(SQConfig.strMergeIndexOutput);

			int numOfPivots = conf.getInt(SQConfig.strNumOfPivots, 10000);
			int K = conf.getInt(SQConfig.strK, 10000);
			MergeIndex ms = new MergeIndex(numOfPivots, K);
			long begin = System.currentTimeMillis();
			ms.mergeAll(input, output);
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("MergeSummary takes " + second + " seconds");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
