package lof.baseline;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Vector;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricDataInputFormat;
import metricspace.MetricKey;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.MetricValue;
import metricspace.Record;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import preprocess.pivotselection.SQConfig;
import util.SortByDist;

import com.infomatiq.jsi.PriorityQueue;

import dataspliting.IndexUtility;

/**
 * Process partition and calculate k-distance for core area (1) at setup of map,
 * read pivots, indexes from the dataset (2) in map function, read an object,
 * get the groups it belong, shuffle it with keys and values (3) in reduce
 * function, compute k-distance for each object in core area.
 * 
 * @author Yizhou modified Luwei
 * 
 */
public class Cal_kdist {
	private static int dim;

	public static class CalKdistMapper extends Mapper<MetricKey, MetricValue, IntWritable, Text> {
		/** matrix space: text, vector */
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		/** value of K */
		int K;
		/** number of pivots */
		int numOfPivots;
		/** number of groups */
		int numOfReducers;
		/** maintain pivots by the order of ids */
		private Vector<Object> pivots;
		/** maintain partitions in R */
		private Vector<Partition> partR;

		private Vector<SortByDist>[] lbOfPartitionSToGroups;

		private float[] newLbOfPartition;
		// private IntWritable interKey;
		float[] gUpperBoundForR;

		private IntWritable interKey = new IntWritable();
		private Text interValue = new Text();

		/** number of object pairs to be computed */
		static enum Counters {
			MapCount, ReplicationOfS
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

		/** read files from local disk */
		private void readCache(IndexUtility indexUtility, Configuration conf, Context context) {

			/** parse files in the cache */
			try {
				URI[] cacheFiles = new URI[0];
				cacheFiles = context.getCacheFiles();

				if (cacheFiles == null || cacheFiles.length < 1)
					return;
				for (URI path : cacheFiles) {
					String filename = path.toString();
					if (filename.endsWith(SQConfig.strPivotExpression)) {
						pivots = indexUtility.readPivotFromFile(filename, metricSpace, dim, conf);
					} else if (filename.endsWith(SQConfig.strIndexExpression1)) {
						partR = indexUtility.readIndexFromFile(filename, conf);
					}
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
		}

		/**
		 * (1) read pivot (2) read index (3) generate candidate pairs
		 */
		@SuppressWarnings("unchecked")
		protected void setup(Context context) throws IOException, InterruptedException {
			int i, j, k;
			Configuration conf = context.getConfiguration();
			numOfReducers = conf.getInt(SQConfig.strNumOfReducers, -1);
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = conf.getInt(SQConfig.strK, 1);
			IndexUtility indexUtility = new IndexUtility();
			readMetricAndMetricSpace(conf);
			readCache(indexUtility, conf, context);
			numOfPivots = pivots.size();
			/** assign partitions in R to groups */

			gUpperBoundForR = new float[numOfPivots];
			/** init the bound */
			lbOfPartitionSToGroups = new Vector[numOfPivots];
			for (i = 0; i < numOfPivots; i++) {
				lbOfPartitionSToGroups[i] = new Vector<SortByDist>();
			}

			// compute the distance matrix
			float[][] distMatrix = new float[numOfPivots][numOfPivots];

			for (j = 0; j < numOfPivots; j++) {

				for (k = 0; k < numOfPivots; k++) {
					if (j == k) {
						distMatrix[j][k] = 0;
						continue;
					}
					distMatrix[j][k] = metric.dist(pivots.get(j), pivots.get(k));
				}
			}

			float[] upperBound = indexUtility.getUpperBound(partR, distMatrix, K);
			newLbOfPartition = new float[numOfPivots];
			for (i = 0; i < numOfPivots; i++) {
				newLbOfPartition[i] = partR.get(i).getMaxRadius() + upperBound[i];
			}
			 initLBOfPartition(distMatrix, upperBound);
			
			 for (i = 0; i < numOfPivots; i++) {
			 Collections.sort(lbOfPartitionSToGroups[i]);
			 }
		}

		// output format: partition id pid,dist,nid,lan,long
		public void map(MetricKey key, MetricValue value, Context context) throws IOException, InterruptedException {
			int pid = key.pid;

			String strValue = value.toString();
			boolean inOtherSupport = false;
			Vector<SortByDist> lbs = lbOfPartitionSToGroups[pid];
			String whoseSupport = "";
			for (SortByDist obj : lbs) {
				if (obj.dist <= key.dist) {
					interKey.set(obj.id);
					whoseSupport += obj.id + SQConfig.sepStrForIDDist;
					interValue.set(key.toString() + ",S," + strValue);
					context.write(interKey, interValue);
					context.getCounter(Counters.MapCount).increment(1);
					context.getCounter(Counters.ReplicationOfS).increment(1);
					inOtherSupport = true;
				} else
					break;
			}
			if (whoseSupport.length() > 0)
				whoseSupport = whoseSupport.substring(0, whoseSupport.length() - 1);
			interKey.set(pid);
			if (inOtherSupport)
				interValue.set(key.toString() + ",Y," + whoseSupport + "," + strValue);
			else
				interValue.set(key.toString() + ",N," + whoseSupport + "," + strValue);
			context.write(interKey, interValue);
			context.getCounter(Counters.MapCount).increment(1);
		}

		// public void map(MetricKey key, MetricValue value, Context context)
		// throws IOException, InterruptedException {
		// int pid = key.pid;
		//
		// String strValue = value.toString();
		// boolean inOtherSupport = false;
		//// Vector<SortByDist> lbs = lbOfPartitionSToGroups[pid];
		// String whoseSupport = "";
		// int numOfPart = partR.size();
		// String []splitStrValue = strValue.split(",");
		// float [] coord =
		// {Float.parseFloat(splitStrValue[1]),Float.parseFloat(splitStrValue[2])};
		// Record curPointRecord = new
		// Record(Integer.parseInt(splitStrValue[0]),coord);
		// for (int i = 0; i < numOfPart; i++){
		// Partition p1 = partR.get(i);
		// if(metric.dist(pivots.get(i),curPointRecord) <= newLbOfPartition[i]
		// ){
		// interKey.set(i);
		// whoseSupport += i + SQConfig.sepStrForIDDist;
		// interValue.set(key.toString()+",S,"+strValue);
		// context.write(interKey, interValue);
		// context.getCounter(Counters.MapCount).increment(1);
		// context.getCounter(Counters.ReplicationOfS)
		// .increment(1);
		// inOtherSupport = true;
		// }
		// }
		//
		// if(whoseSupport.length()>0)
		// whoseSupport = whoseSupport.substring(0, whoseSupport.length()-1);
		// interKey.set(pid);
		// if(inOtherSupport)
		// interValue.set(key.toString()+",Y,"+whoseSupport+","+strValue);
		// else
		// interValue.set(key.toString()+",N,"+ whoseSupport+","+strValue);
		// context.write(interKey, interValue);
		// context.getCounter(Counters.MapCount).increment(1);
		// }

		private void initLBOfPartition(float[][] distMatrix, float[] upperBoundForR) {
			int i, j, pidInS, pidInR;
			float dist, lb, minLB;

			for (i = 0; i < numOfPivots; i++) {
				pidInS = i;

				for (j = 0; j < numOfPivots; j++) {
					if (i == j) {
						continue;
					}
					minLB = partR.get(pidInS).max_r + 1;
					pidInR = j;
					dist = distMatrix[j][i];
					lb = dist - partR.get(pidInR).max_r - upperBoundForR[j];
					if (lb < partR.get(pidInS).min_r) {
						minLB = partR.get(pidInS).min_r;
					} else {
						minLB = lb;
					}
					SortByDist obj = new SortByDist(pidInR, minLB);
					lbOfPartitionSToGroups[pidInS].add(obj);
				}
			}
		}
	}

	public static class CalKdistReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		int K;
		IntWritable outputKey = new IntWritable();
		Text outputValue = new Text();

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

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			readMetricAndMetricSpace(conf);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		}

		private MetricObject parseObject(int key, String strInput) {
			// System.out.println(strInput);
			int partition_id = key;
			String[] inputSplits = strInput.split(",");
			int pid = Integer.valueOf(inputSplits[0]);
			float dist = Float.valueOf(inputSplits[1]);
			char type = inputSplits[2].charAt(0);
			String whoseSupport = "";
			int offset;
			if (type != 'S') {
				whoseSupport = inputSplits[3];
				offset = inputSplits[0].length() + inputSplits[1].length() + inputSplits[3].length() + 5;
			} else
				offset = inputSplits[0].length() + inputSplits[1].length() + 4;
			// System.out.println(whoseSupport+ " ----- "+
			// strInput.substring(offset,strInput.length()));
			Object obj = metricSpace.readObject(strInput.substring(offset, strInput.length()), dim);
			return new MetricObject(pid, dist, type, partition_id, whoseSupport, obj);
		}

		/**
		 * find knn for each string in the key.pid format of each value in
		 * values
		 * 
		 */
		@SuppressWarnings("unchecked")
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Vector<MetricObject> coreData = new Vector();
			Vector<MetricObject> supportData = new Vector();
			for (Text value : values) {
				MetricObject mo = parseObject(key.get(), value.toString());
				if (mo.type == 'S')
					supportData.add(mo);
				else
					coreData.add(mo);
			}
			long begin = System.currentTimeMillis();
			for (MetricObject o_S : coreData) {
				findKNNForSingleObject(context, o_S, coreData, supportData);
			}
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("computation time " + " takes " + second + " seconds");
		}

		/**
		 * need optimization
		 * 
		 * @throws InterruptedException
		 */
		private void findKNNForSingleObject(Context context, MetricObject o_R, Vector<MetricObject> coreData,
				Vector<MetricObject> supportData) throws IOException, InterruptedException {
			float dist;
			PriorityQueue pq = new PriorityQueue(PriorityQueue.SORT_ORDER_DESCENDING);
			/** compute and update core area */
			float theta = Float.POSITIVE_INFINITY;
			for (MetricObject o_S : coreData) {
				if (o_R.obj.equals(o_S.obj)) {
					// System.out.println("same----skip");
					continue;
				}
				dist = metric.dist(o_R.obj, o_S.obj);
				if (pq.size() < K) {
					pq.insert(metricSpace.getID(o_S.obj), dist);
					theta = pq.getPriority();
				} else if (dist < theta) {
					pq.pop();
					pq.insert(metricSpace.getID(o_S.obj), dist);
					theta = pq.getPriority();
				}
			}
			/** compute and update support area */
			for (MetricObject o_S : supportData) {
				if (o_R.obj.equals(o_S.obj)) {
					// System.out.println("same----skip");
					continue;
				}
				dist = metric.dist(o_R.obj, o_S.obj);
				if (pq.size() < K) {
					pq.insert(metricSpace.getID(o_S.obj), dist);
					theta = pq.getPriority();
				} else if (dist < theta) {
					pq.pop();
					pq.insert(metricSpace.getID(o_S.obj), dist);
					theta = pq.getPriority();
				}
			}

			String line = "";
			// output format key:nid value: partition id, pid,
			// k-distance,whoseSupport, (KNN's nid and dist)
			line += o_R.partition_id + SQConfig.sepStrForRecord + o_R.pid + SQConfig.sepStrForRecord + pq.getPriority()
					+ SQConfig.sepStrForRecord + o_R.whoseSupport + SQConfig.sepStrForRecord;

			if (pq.size() > 0) {
				line += pq.getValue() + SQConfig.sepStrForIDDist + pq.getPriority();
				pq.pop();
			}
			while (pq.size() > 0) {
				line += SQConfig.sepStrForRecord + pq.getValue() + SQConfig.sepStrForIDDist + pq.getPriority();
				pq.pop();
			}
			outputValue.set(line);
			outputKey.set(metricSpace.getID(o_R.obj));
			context.write(outputKey, outputValue);
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Calculate k-distance");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(Cal_kdist.class);
		job.setMapperClass(CalKdistMapper.class);

		job.setOutputKeyClass(IntWritable.class); ////////////////////////////////////
		job.setOutputValueClass(Text.class); //////////////////////////
		job.setOutputFormatClass(TextOutputFormat.class); ////////////////////
		job.setInputFormatClass(MetricDataInputFormat.class);
		job.setReducerClass(CalKdistReducer.class);
		//job.setNumReduceTasks(Integer.parseInt(conf.get(SQConfig.strNumOfReducers)));
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strLofInput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strKdistanceOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strKdistanceOutput)));

		job.addCacheFile(new URI(strFSName + conf.get(SQConfig.strPivotInput)));
		job.addCacheFile(new URI(strFSName + conf.get(SQConfig.strMergeIndexOutput) + Path.SEPARATOR + "summary"
				+ SQConfig.strIndexExpression1));
		// MultipleOutputs.addNamedOutput(job, "details",
		// TextOutputFormat.class,
		// IntWritable.class, Text.class);
		// MultipleOutputs.addNamedOutput(job, "kdistance",
		// TextOutputFormat.class,
		// IntWritable.class, Text.class);
		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.strLofInput));
		System.err.println("output path: " + conf.get(SQConfig.strKdistanceOutput));
		System.err.println("pivot file: " + conf.get(SQConfig.strPivotInput));
		System.err.println("index file 1: " + conf.get(SQConfig.strIndexOutput) + Path.SEPARATOR + "summary"
				+ SQConfig.strIndexExpression1);
		// System.err.println("group file: " +
		// conf.get(SQConfig.strGroupOutput));
		System.err.println("dataspace: " + conf.get(SQConfig.strMetricSpace));
		System.err.println("metric: " + conf.get(SQConfig.strMetric));
		System.err.println("value of K: " + conf.get(SQConfig.strK));
		System.err.println("# of groups: " + conf.get(SQConfig.strNumOfReducers));
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));

		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		Cal_kdist rs = new Cal_kdist();
		rs.run(args);
	}
}
