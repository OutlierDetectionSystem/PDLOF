package lof.baseline;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import dataspliting.IndexUtility;
import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricDataInputFormat;
import metricspace.MetricKey;
import metricspace.MetricObject;
import metricspace.MetricSpaceUtility;
import metricspace.MetricValue;
import metricspace.Record;
import preprocess.pivotselection.SQConfig;
import util.SortByDist;

public class Cal_lrd {
	private static int dim;

	public static class CalLRDMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/** value of K */
		int K;
		/** number of groups */
		int numOfReducers;

		private IntWritable interKey = new IntWritable();
		private Text interValue = new Text();

		@SuppressWarnings("unchecked")
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numOfReducers = conf.getInt(SQConfig.strNumOfReducers, -1);
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = conf.getInt(SQConfig.strK, 1);
		}

		/**
		 * used to calculate LRD same partition as the first round input format:
		 * key: nid || value: partition id, pid, k-distance, whoseSupport,
		 * (KNN's nid and dist) output format: (Core area)key: partition id ||
		 * value: nid, pid, type(S or C), k-distance,whoseSupport, (KNN's nid
		 * and dist) (Support area)key: partition id || value: nid, pid, type(S
		 * or C), k-distance
		 * 
		 * @author yizhouyan
		 */

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println("key: "+ key + " ----- value: "+
			// value.toString());
			String[] valuePart = value.toString().split(SQConfig.sepStrForKeyValue);
			int nid = Integer.valueOf(valuePart[0]);
			String[] strValue = valuePart[1].split(SQConfig.sepStrForRecord);
			int Core_partition_id = Integer.valueOf(strValue[0]);
			int pid = Integer.valueOf(strValue[1]);
			float kdist = Float.valueOf(strValue[2]);

			int offset = strValue[0].length() + strValue[1].length() + strValue[2].length() + strValue[3].length() + 4;
			String knn_id_dist = valuePart[1].substring(offset, valuePart[1].length());

			// output Core partition node
			interKey.set(Core_partition_id);
			interValue.set(nid + "," + pid + ",C," + kdist + "," + strValue[3] + "," + knn_id_dist);
			context.write(interKey, interValue);

			// output Support partition node
			if (strValue[3].length() != 0) {
				String[] whosePar = strValue[3].split(SQConfig.sepSplitForIDDist);
				for (int i = 0; i < whosePar.length; i++) {
					int tempid = Integer.valueOf(whosePar[i]);
					interKey.set(tempid);
					interValue.set(nid + "," + pid + ",S," + kdist);
					context.write(interKey, interValue);
				}
			}
		}
	}

	public static class CalLRDReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		int K;
		IntWritable outputKey = new IntWritable();
		Text outputValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			dim = conf.getInt(SQConfig.strDimExpression, -1);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
		}

		/**
		 * used to calculate LRD input format: (Core area)key: partition id ||
		 * value: nid, pid, type(S or C), k-distance, whoseSupport, (KNN's nid
		 * and dist) (Support area)key: partition id || value: nid, pid, type(S
		 * or C), k-distance
		 * 
		 * @author yizhouyan
		 */
		private MetricObject parseObject(int key, String strInput) {
			// System.out.println(strInput);
			int partition_id = key;
			String[] inputSplits = strInput.split(",");
			Record obj = new Record(Integer.valueOf(inputSplits[0]));

			int pid = Integer.valueOf(inputSplits[1]);
			char type = inputSplits[2].charAt(0);
			float kdistance = Float.valueOf(inputSplits[3]);
			String whoseSupport = "";
			String KNN = "";
			int offset;
			if (type != 'S') {
				offset = inputSplits[0].length() + inputSplits[1].length() + inputSplits[3].length()
						+ inputSplits[4].length() + 6;
				KNN = strInput.substring(offset, strInput.length());
				whoseSupport = inputSplits[4];
			}
			return new MetricObject(pid, 0, type, partition_id, whoseSupport, obj, kdistance, KNN);
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
				// System.out.println("RID ==== "+ ((Record) mo.obj).getRId());
				// System.out.println("kdist ==== "+ mo.kdist);
				// System.out.println("whoseSupport ==== "+ mo.whoseSupport);
				// System.out.println("KNN ==== "+ mo.KNN);
				if (mo.type == 'S')
					supportData.add(mo);
				else
					coreData.add(mo);
			}
			HashMap<Integer, Float> hm_kdistance = new HashMap();
			for (MetricObject o_S : coreData) {
				hm_kdistance.put(((Record) o_S.obj).getRId(), o_S.kdist);
			}
			for (MetricObject o_S : supportData) {
				hm_kdistance.put(((Record) o_S.obj).getRId(), o_S.kdist);
			}

			long begin = System.currentTimeMillis();
			for (MetricObject o_S : coreData) {
				CalLRDForSingleObject(context, o_S, hm_kdistance);
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
		private void CalLRDForSingleObject(Context context, MetricObject o_S, HashMap<Integer, Float> hm)
				throws IOException, InterruptedException {

			float[] reachDist = new float[K];
			float lrd_core = 0.0f;
			String[] splitKNN = o_S.KNN.split(SQConfig.sepStrForRecord);
			for (int i = 0; i < splitKNN.length; i++) {
				// System.out.println("splitKNN-----" + splitKNN[i]);
				String tempString = splitKNN[i];
				String[] tempSplit = tempString.split(SQConfig.sepSplitForIDDist);
				// System.out.println("splitKNN-----" + tempSplit[0] +
				// "-----"+tempSplit.length);
				float temp_dist = Float.valueOf(tempSplit[1]);
				float temp_reach_dist = Math.max(temp_dist, hm.get(Integer.valueOf(tempSplit[0])));
				lrd_core += temp_reach_dist;
			}
			lrd_core = 1.0f / (lrd_core / K * 1.0f);
			// System.out.println("LRD-----" + lrd_core);
			String line = "";
			// output format key:nid value: partition id, pid, lrd
			// ,whoseSupport, (KNN's nid and dist)
			line += o_S.partition_id + SQConfig.sepStrForRecord + o_S.pid + SQConfig.sepStrForRecord + lrd_core
					+ SQConfig.sepStrForRecord + o_S.whoseSupport + SQConfig.sepStrForRecord + o_S.KNN;

			outputValue.set(line);
			outputKey.set(((Record) o_S.obj).getRId());
			context.write(outputKey, outputValue);
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.addResource(new
		// Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		// conf.addResource(new
		// Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Calculate lrd");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(Cal_lrd.class);
		job.setMapperClass(CalLRDMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class); ////////////////////
		job.setReducerClass(CalLRDReducer.class);
		// job.setNumReduceTasks(2);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.strKdistanceOutput)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strLRDOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strLRDOutput)));

		/** print job parameter */
		System.err.println("input path: " + conf.get(SQConfig.strKdistanceOutput));
		System.err.println("output path: " + conf.get(SQConfig.strLRDOutput));
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
		Cal_lrd rs = new Cal_lrd();
		rs.run(args);
	}
}
