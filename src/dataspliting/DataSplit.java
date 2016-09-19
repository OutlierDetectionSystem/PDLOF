package dataspliting;

import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricKey;
import metricspace.MetricSpaceUtility;
import preprocess.pivotselection.SQConfig;

/**
 * This class is used to: (1) split the input dataset into partition according
 * to the pivots (2) compute the summary for each partition over each reducer.
 * Hence, we need another step to compute the summary for the whole partition
 * across all reducers.
 * 
 * @author Yizhou Yan modified on luwei's code
 *
 */
public class DataSplit {
	public static class DataSplitMapper extends Mapper<Object, Text, MetricKey, Text> {
		/**
		 * VectorSpace and L1 are set as the default metric space and metric.
		 */
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		private int dim;
		/** store pivots */
		private int numOfPivots;
		private Vector<Object> pivots;
		private Vector<Integer> numOfObjects = new Vector<Integer>();
		/**
		 * a summary file
		 */
		private float[] min_R, max_R;
		private int[] numOfObjects_R, sizeOfObjects_R;
		private int []numOfObjectsLarger11G;
		private PriorityQueue<Float>[] KNNObjectsToPivots_S;
		/** KNN */
		int K;
		/** intermediate key */
		private MetricKey interKey;

		/** threshold G */
		private int G = 10;

		/** number of object pairs to be computed */
		static enum Counters {
			sum
		}

		/**
		 * get MetricSpace and metric from configuration
		 * 
		 * @param conf
		 * @throws IOException
		 */
		private void readMetricAndMetricSpace(Configuration conf) throws IOException {
			try {
				metricSpace = MetricSpaceUtility.getMetricSpace(conf.get(SQConfig.strMetricSpace));
				metric = MetricSpaceUtility.getMetric(conf.get(SQConfig.strMetric));
				metricSpace.setMetric(metric);
			} catch (InstantiationException e) {
				throw new IOException("InstantiationException");
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				throw new IOException("IllegalAccessException");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new IOException("ClassNotFoundException");
			}
		}

		/**
		 * read pivots from input
		 * 
		 * @param conf
		 */
		private void readPivot(Configuration conf, Context context) throws IOException {
			IndexUtility indexUtility = new IndexUtility();
			URI[] pivotFiles = new URI[0];

			pivotFiles = context.getCacheFiles();
			if (pivotFiles == null || pivotFiles.length < 1)
				throw new IOException("No pivots are provided!");

			for (URI path : pivotFiles) {
				String filename = path.toString();
				// String filename = conf.get(SQConfig.strPivotInput);
				if (filename.endsWith(SQConfig.strPivotExpression))
					pivots = indexUtility.readPivotFromFile(filename, metricSpace, dim, conf);
			}
		}

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

		@SuppressWarnings("unchecked")
		private void initSummaryInfo() {
			min_R = new float[numOfPivots];
			max_R = new float[numOfPivots];
			numOfObjects_R = new int[numOfPivots];
			sizeOfObjects_R = new int[numOfPivots];
			KNNObjectsToPivots_S = new PriorityQueue[numOfPivots];
			numOfObjectsLarger11G = new int[numOfPivots];
			for (int i = 0; i < numOfPivots; i++) {
				min_R[i] = Float.MAX_VALUE;
				max_R[i] = 0;
				numOfObjects_R[i] = 0;
				sizeOfObjects_R[i] = 0;
				numOfObjectsLarger11G[i] = 0;
				KNNObjectsToPivots_S[i] = new PriorityQueue<Float>(K, new ASCPQ<Float>());
				numOfObjects.add(0);
			}
		}

		/**
		 * Called once at the beginning of the task. In setup, we (1) init
		 * metric and metric space (2) read pivots (3) init matrix
		 */
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			/** get K */
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
			/** get dim */
			dim = conf.getInt(SQConfig.strDimExpression, 10);
			G = conf.getInt(SQConfig.strDistOfG, 10);
			/** read metric */
			readMetricAndMetricSpace(conf);
			/** read pivot set */
			readPivot(conf, context);
			numOfPivots = pivots.size();
			initSummaryInfo();
		}

		/**
		 * @param o
		 *            : find the closest pivot for the input object
		 * @return: pivot id + distance (Bytes.SIZEOF_INT + Bytes.SIZEOF_float)
		 * @throws IOException
		 */
		private MetricKey getKey(Object o) throws IOException {
			int i, closestPivot = 0;
			float closestDist = Float.MAX_VALUE;
			float tmpDist;

			/** find the closest pivot */
			for (i = 0; i < numOfPivots; i++) {
				tmpDist = metric.dist(pivots.get(i), o);
				if (tmpDist < closestDist) {
					closestDist = tmpDist;
					closestPivot = i;
				} else if (closestDist == tmpDist && numOfObjects.get(closestPivot) > numOfObjects.get(i)) {
					closestPivot = i;
				}
			}

			/** set intermediate key */
//			if (closestDist > 11 * G)
//				return null;
			MetricKey metricKey = new MetricKey();
			metricKey.dist = closestDist;
			metricKey.pid = closestPivot;

			// plus 1 for the number of objects in the partition (pivotId)
			numOfObjects.set(closestPivot, numOfObjects.get(closestPivot) + 1);
			return metricKey;
		}

		private void updateSummaryInfo(MetricKey interKey, int len) {
			int pid = interKey.pid;
			float dist = interKey.dist;
			if (dist < min_R[pid])
				min_R[pid] = dist;
			if (dist > max_R[pid])
				max_R[pid] = dist;
			numOfObjects_R[pid]++;
			if(dist > 11*G)
				numOfObjectsLarger11G[pid]++;
			sizeOfObjects_R[pid] += len;
			if (KNNObjectsToPivots_S[pid].size() < K) {
				KNNObjectsToPivots_S[pid].add(dist);
			} else {
				if (dist < KNNObjectsToPivots_S[pid].peek()) {
					KNNObjectsToPivots_S[pid].remove();
					KNNObjectsToPivots_S[pid].add(dist);
				}
			}
		}

		/**
		 * We generate an intermediate key and value (1) key: the combination of
		 * the pivot id (int) and distance (float) (2) value: the input object
		 * 
		 * @key: offset of the source file
		 * @value: a single object in the metric space
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			int pos = line.indexOf(",");

			/** parse the object */

			Object o = metricSpace.readObject(line, dim);
			/** generate the intermediate key */
			interKey = getKey(o);
			if (interKey != null) {
				/** update the summary info */
				updateSummaryInfo(interKey, line.length());
				context.write(interKey, new Text(line)); // format: pid, dist to
															// pid, nid, info
			}
		}

		/**
		 * output summary infomration
		 */
		protected void cleanup(Context context) throws IOException, InterruptedException {
			/** output the summary of R */
			outputSummary(context);
		}

		private void outputSummary(Context context) throws IOException {
			FileSystem fs;
			FSDataOutputStream currentStream = null;
			Configuration conf = context.getConfiguration();

			/** create output name */
			String summaryOutput = conf.get(SQConfig.strIndexOutput);
			int partition = context.getConfiguration().getInt("mapred.task.partition", -1);
			NumberFormat numberFormat = NumberFormat.getInstance();
			numberFormat.setMinimumIntegerDigits(5);
			numberFormat.setGroupingUsed(false);
			summaryOutput = summaryOutput + "/summary" + "-m-" + numberFormat.format(partition);

			fs = FileSystem.get(conf);
			Path path = new Path(summaryOutput);
			currentStream = fs.create(path, true);
			String line;

			/** writhe the summary information for R */
			for (int i = 0; i < numOfPivots; i++) {
				if (numOfObjects_R[i] == 0)
					continue;
				line = Integer.toString(i) + "," + min_R[i] + "," + max_R[i] + "," + sizeOfObjects_R[i] + ","
						+ numOfObjects_R[i] + "," + numOfObjectsLarger11G[i];
				PriorityQueue<Float> pq = KNNObjectsToPivots_S[i];
				while (pq.size() > 0) {
					line += "," + pq.remove();
				}
				line += "\n";
				currentStream.writeBytes(line);
			}
			currentStream.close();
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		String strFSName = conf.get("fs.default.name");

		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.dataset));
		System.err.println("output path: " + conf.get(SQConfig.strLofInput));
		System.err.println("pivot path: " + conf.get(SQConfig.strPivotInput));
		System.err.println("dataspace: " + conf.get(SQConfig.strMetricSpace));
		System.err.println("metric: " + conf.get(SQConfig.strMetric));
		System.err.println("index out: " + conf.get(SQConfig.strIndexOutput));
		System.err.println("value of K: " + conf.get(SQConfig.strK));
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, -1));
		System.err.println("# of reducers: " + conf.getInt(SQConfig.strNumOfReducers, -1));

		/** set job parameter */
		Job job = Job.getInstance(conf, "data splitting");
		job.setJarByClass(DataSplit.class);
		job.setMapperClass(DataSplitMapper.class);
		job.setOutputKeyClass(MetricKey.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strLofInput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strLofInput)));
		job.addCacheFile(new URI(strFSName + conf.get(SQConfig.strPivotInput)));

		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		DataSplit DataSplit = new DataSplit();
		DataSplit.run(args);
	}
}
