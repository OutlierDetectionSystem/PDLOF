package preprocess.pivotselection.kcover.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
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
import preprocess.pivotselection.kcenter.KCenterFinalRound;
import preprocess.pivotselection.kcenter.KCenterFinalRound.KCenterFinalRoundMapper;
import preprocess.pivotselection.kcenter.KCenterFinalRound.KCenterFinalRoundReducer;
import preprocess.pivotselection.kcover.DataPoint;
import preprocess.pivotselection.kcover.PointCluster;
import util.kdtree.KDTree;

public class KCoverFinalRoundWithKDTree {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Dec 31, 2015
	 */

	public static class KCoverFinalRoundMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text dat = new Text(value.toString());
			context.write(new IntWritable(1), dat);
		}
	}

	/**
	 * @author yizhouyan
	 *
	 */
	public static class KCoverFinalRoundReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
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
		/** The domains. (set by user) */
		private static double domainRange;

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
			double domainsMin = conf.getDouble(SQConfig.strDomainMin, 0.0);
			double domainsMax = conf.getDouble(SQConfig.strDomainMax, 10001);
			domainRange = domainsMax - domainsMin;
		}

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<DataPoint> pointList = new ArrayList<DataPoint>();
			KDTree searchListTree = new KDTree(num_dims);
			// read in points
			for (Text oneValue : values) {
				String sCurrentLine = oneValue.toString();
				Object obj = metricSpace.readObject(sCurrentLine, num_dims);
				DataPoint currentPoint = new DataPoint(obj);
				PointCluster currentCluster = new PointCluster(currentPoint);
				currentPoint.setCluster(currentCluster);
				pointList.add(currentPoint);
				searchListTree.insert(((Record) currentPoint.getPointRecord()).getValueDouble(), currentPoint);
			}
			int treeSize = searchListTree.getTreeSize();
			System.out.println("Point size: " + treeSize);

			if (pointList.size() <= numOfPivots) {
				// output all points
				for (DataPoint r : pointList) {
					context.write(NullWritable.get(), new Text(r.getPointRecord().toString()));
				}
				return;
			}
			double radius = Math.pow((Math.pow(domainRange, num_dims) / treeSize), 1.0 / num_dims)/2;
			int previousSearchSize = treeSize;

			while (treeSize > numOfPivots) {
				// for each point and radius r, count how many point is covered
				int maxNumCoverPoint = 0;
				ArrayList<Object> maxCoverDataPointInSearchingList = new ArrayList<Object>();
				int iStarPointIndex = -1;

				for (int i = 0; i < pointList.size(); i++) {
					double[] tempLowk = new double[num_dims];
					double[] tempUppk = new double[num_dims];
					double[] pointCoordinate = ((Record) pointList.get(i).getPointRecord()).getValueDouble();
					for (int j = 0; j < num_dims; j++) {
						tempLowk[j] = Math.max(0, pointCoordinate[j] - radius);
						tempUppk[j] = Math.min(domainRange, pointCoordinate[j] + radius);
					}
					Object[] tempCoverList = searchListTree.range(tempLowk, tempUppk);
					 int countCover = Math.min(tempCoverList.length - 1,
					 treeSize - numOfPivots);
//					int countCover = tempCoverList.length - 1;

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
					// update all points in the cluster: point to the
					// newcluster,
					// and remove points from the searchList
					newCenter.setCluster(newCluster);
					// System.out.println("Delete count: " +
					// newPointList.size());
					for (DataPoint pointId : newPointList.values()) {
						pointId.setCluster(newCluster);
						try {
							searchListTree.delete(((Record) pointId.getPointRecord()).getValueDouble());
						} catch (RuntimeException e) {
						}
					}
					searchListTree.insert(((Record) newCenter.getPointRecord()).getValueDouble(), newCenter);
					treeSize = searchListTree.getTreeSize();

//					System.out.println("previous: " + previousSearchSize + ", now: " + treeSize);
					if (previousSearchSize == treeSize) {
						radius = radius * 1.2;
					} else {
						radius = Math.pow((Math.pow(domainRange, num_dims) / treeSize), 1.0 / num_dims)/2;
						previousSearchSize = treeSize;
					}
				} else
					radius = radius * 2;
			} // end while

			double[] lowk = new double[num_dims];
			double[] uppk = new double[num_dims];
			for (int i = 0; i < num_dims; i++) {
				lowk[i] = 0;
				uppk[i] = domainRange;
			}
			Object[] finalResult = searchListTree.range(lowk, uppk);

			for (int i = 0; i < finalResult.length; i++) {
				context.write(NullWritable.get(), new Text(
						((DataPoint) finalResult[i]).getCluster().getCentralPoint().getPointRecord().toString()));
			} // end for
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "K cover final round");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(KCoverFinalRoundWithKDTree.class);
		job.setMapperClass(KCoverFinalRoundMapper.class);
		job.setReducerClass(KCoverFinalRoundReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.kCovertempFile)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.kCoverFinalOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.kCoverFinalOutput)));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		KCoverFinalRoundWithKDTree rescale = new KCoverFinalRoundWithKDTree();
		rescale.run(args);
	}

}
