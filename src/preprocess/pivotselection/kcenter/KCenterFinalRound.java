package preprocess.pivotselection.kcenter;

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
		}

		/**
		 * default Reduce class.
		 * 
		 * @author Yizhou Yan
		 * @version Dec 31, 2015
		 * @throws InterruptedException
		 */
		class PointTuple{
			private Record point;
			private float distToPivot;
			public PointTuple(Record point, float distToPivot){
				this.point = point;
				this.distToPivot = distToPivot;
			}
			public String getRecordString(){
				return point.toString();
			}
			public Record getPoint(){
				return point;
			}
			public float getDistToPivot(){
				return distToPivot;
			}
			public void setDistToPivot(float newDistToPivot){
				this.distToPivot = newDistToPivot;
			}
		}
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int num_data = 0; // number of data
			ArrayList<PointTuple> allPoints = new ArrayList<PointTuple>();
			// collect data
			for (Text oneValue : values) {
				String line = oneValue.toString();
				// System.out.println("key: "+ key.toString()+ " value: "+line);
				num_data++;
				Object obj = metricSpace.readObject(line, num_dims);
				allPoints.add(new PointTuple((Record) obj, Float.POSITIVE_INFINITY));
			} // end collection data
				// if contains less than K centers in that partition, write out
				// all these points
			if (num_data <= numOfPivots) {
				for (PointTuple r : allPoints) {
					context.write(NullWritable.get(), new Text(r.getRecordString()));
				}
				return;
			}
			// begin selection
			List<Record> pivots = new ArrayList<Record>();
			Record newPivot = allPoints.get(0).getPoint();
			pivots.add(allPoints.get(0).getPoint());
			allPoints.remove(0);
			/** when the number of pivots is less than K centers, greedy add pivots
			 *  Assign all points to the nearest pivot and record the one that has the maximum distance to its pivot
			 *  In the end, select the furthest one as the new pivot
			 */
			while(pivots.size() < numOfPivots){
				float maxDist = 0.0f;
				int indexOfNewCandidate = -1;
				// first assign all other points to the new pivot, change if it nears the new pivot
				// at the same time, track the maxDist
				for(int i = 0; i < allPoints.size() ;i++){
					Record tempPoint = allPoints.get(i).getPoint();
					float distToNewPivot = metric.dist(newPivot,tempPoint);
					if(distToNewPivot < allPoints.get(i).getDistToPivot()){
						allPoints.get(i).setDistToPivot(distToNewPivot);
					}
					if(allPoints.get(i).getDistToPivot() > maxDist){
						maxDist = allPoints.get(i).getDistToPivot();
						indexOfNewCandidate = i;
					}
				} // end for
//				System.out.println("# of pivots =  "+ pivots.size() + ", Max kdistance = " + maxDist);
				// set the candidate to be the new pivot
				newPivot = allPoints.get(indexOfNewCandidate).getPoint();
				pivots.add(allPoints.get(indexOfNewCandidate).getPoint());
				allPoints.remove(indexOfNewCandidate);
			}
			// output all pivots
			for (Record r : pivots) {
				context.write(NullWritable.get(), new Text(r.toString().substring(0, r.toString().length())));
			}
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

	public static void main(String[] args) throws Exception{
		KCenterFinalRound rescale = new KCenterFinalRound();
		rescale.run(args);
	}
}
