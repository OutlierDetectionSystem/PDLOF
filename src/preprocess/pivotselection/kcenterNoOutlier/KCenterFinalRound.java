package preprocess.pivotselection.kcenterNoOutlier;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import preprocess.pivotselection.SQConfig;

public class KCenterFinalRound {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Dec 31, 2015
	 */

	public static class KCenterFinalRoundMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text dat = new Text(value.toString());
			context.write(new IntWritable(1), dat);
		}
	}

	/**
	 * @author yizhouyan
	 *
	 */
	public static class KCenterFinalRoundReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		/** number of pivots */
		private static int numOfPivots;
		/** matrix space: text, vector */
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		private static int valueOfG;
		private MultipleOutputs<NullWritable, Text> mos;
		private static String outputPath;
		private static int K = 3;
		
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
			num_dims = conf.getInt(SQConfig.strDimExpression, -1);
			readMetricAndMetricSpace(conf);
			numOfPivots = conf.getInt(SQConfig.strNumOfPivots, 100);
			valueOfG = conf.getInt(SQConfig.strDistOfG, 100);
			mos = new MultipleOutputs<NullWritable, Text>(context);
			outputPath = conf.get(SQConfig.kCenterFinalOutput);
			K = conf.getInt(SQConfig.strK, 3);
		}

		/**
		 * default Reduce class.
		 * 
		 * @author Yizhou Yan
		 * @version Dec 31, 2015
		 * @throws InterruptedException
		 */
		class PointTuple {
			private Record point;
			private int weight;
			private int cumulatedWeight;

			public PointTuple(Record point, int weight) {
				this.point = point;
				this.weight = weight;
			}

			public String getRecordString() {
				return point.toString();
			}

			public Record getPoint() {
				return point;
			}

			public int getWeight() {
				return weight;
			}

			public void setCumulatedWeight(int cum_weight) {
				this.cumulatedWeight = cum_weight;
			}
		}

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int num_data = 0; // number of data
			ArrayList<PointTuple> allPoints = new ArrayList<PointTuple>();
			// collect data
			for (Text oneValue : values) {
				String line = oneValue.toString();
				num_data++;
				Object obj = metricSpace.readObject(line.substring(0, line.lastIndexOf(",")), num_dims);
				int weight = Integer.parseInt(line.substring(line.lastIndexOf(",") + 1, line.length()));
				allPoints.add(new PointTuple((Record) obj, weight));
			} // end collection data

			ArrayList<PointTuple> copyAllPoints = new ArrayList<PointTuple>(allPoints);
			ArrayList<Record> pivots = new ArrayList<Record>();
			while (pivots.size() < numOfPivots) {
				System.out.println("Pivot size: " + pivots.size() + ", Number of points remained: " + copyAllPoints.size());
				// for every point in allPoints, compute how many points can include in copyAllPoints, and save to each point
				// record the one has the largest cumulative weights to be the new pivot
				int indexOfNewCandidate = -1;
				int maxNumOfPointsSummary = 0;
				for(int i = 0; i < allPoints.size(); i++){
					int numOfPointsSummary = 0;
					Record curPoint = allPoints.get(i).getPoint();
					for(int j = 0; j< copyAllPoints.size(); j++){
						Record tempPoint = copyAllPoints.get(j).getPoint();
						float distToNewPivot = metric.dist(curPoint,tempPoint);
						if(distToNewPivot <= 5 * valueOfG){
							numOfPointsSummary += copyAllPoints.get(j).getWeight();
						}
					}
					if(numOfPointsSummary > maxNumOfPointsSummary){
						indexOfNewCandidate = i;
						maxNumOfPointsSummary = numOfPointsSummary;
					}
				} // end traverse all points
				if(maxNumOfPointsSummary <= K * 5)
					break;
				Record newPivotPoint = allPoints.get(indexOfNewCandidate).getPoint();
				// add new pivot
				pivots.add(newPivotPoint);
				// for the new pivot, remove all that is inside 11 G
				ArrayList<PointTuple> remainPoints = new ArrayList<PointTuple>();
				for(int j = 0; j < copyAllPoints.size() ; j++){
					Record tempPoint = copyAllPoints.get(j).getPoint();
					float distToNewPivot = metric.dist(newPivotPoint,tempPoint);
					if(distToNewPivot > 11 * valueOfG){
						remainPoints.add(copyAllPoints.get(j));
					}
				}
				copyAllPoints.clear();
				copyAllPoints.addAll(remainPoints);
				// remove the new pivot from allPoints
				allPoints.remove(indexOfNewCandidate);
			}
			System.out.println("Outliers detected: " + copyAllPoints.size());
			// output all pivots
			for (Record r : pivots) {
				mos.write(NullWritable.get(),new Text(r.toString()) , outputPath +"/Pivots/"+pivots.size()+"_"+ copyAllPoints.size());
			}
			// output all outliers
			for (PointTuple r : copyAllPoints) {
				mos.write(NullWritable.get(),new Text(r.getPoint().toString()) , outputPath + "/Outliers/"+pivots.size()+"_"+ copyAllPoints.size());
			}
		}
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "K center first round");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(KCenterFinalRound.class);
		job.setMapperClass(KCenterFinalRoundMapper.class);
		job.setReducerClass(KCenterFinalRoundReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleOutputs.addNamedOutput(job, "outliers", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "kcenterpivots", TextOutputFormat.class, NullWritable.class, Text.class);
		
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.kCentertempFile)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.kCenterFinalOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.kCenterFinalOutput)));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		KCenterFinalRound rescale = new KCenterFinalRound();
		rescale.run(args);
	}
}
