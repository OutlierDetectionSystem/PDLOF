package preprocess.pivotselection.kcover.distributed;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import preprocess.pivotselection.kcenter.KCenterSeperate;
import preprocess.pivotselection.kcenter.KCenterSeperate.KCenterSeperateMapper;
import preprocess.pivotselection.kcenter.KCenterSeperate.KCenterSeperateReducer;
import preprocess.pivotselection.kcover.DataPoint;
import preprocess.pivotselection.kcover.PointCluster;

public class KCoverSeperate {
	public static class KCoverSeperateMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		/**
		 * number of small cells per dimension: when come with a node, map to a
		 * range (divide the domain into small_cell_num_per_dim) (set by user)
		 */
		public static int partitionNumPerDim = 201;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			partitionNumPerDim = conf.getInt(SQConfig.strNumOfPartitions, 201);
		}

		/**
		 * Mapper -2 : Randomly select partitions for each point
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text dat = new Text(value.toString());
			Random r = new Random();
			IntWritable key_id = new IntWritable(r.nextInt((int) Math.pow(partitionNumPerDim, num_dims)));
			context.write(key_id, dat);
		}
	} // end mapper

	/**
	 * @author yizhouyan
	 *
	 */
	public static class KCoverSeperateReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
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
			HashMap<Integer, DataPoint> searchList = new HashMap<Integer, DataPoint>();
			// read in points
			for (Text oneValue : values) {
				String sCurrentLine = oneValue.toString();
				Object obj = metricSpace.readObject(sCurrentLine, num_dims);
				DataPoint currentPoint = new DataPoint(obj);
				PointCluster currentCluster = new PointCluster(currentPoint);
				currentPoint.setCluster(currentCluster);
				pointList.add(currentPoint);
				searchList.put(((Record) currentPoint.getPointRecord()).getRId(), currentPoint);
			}

			System.out.println("Point size: " + searchList.size());

			if (pointList.size() <= numOfPivots) {
				// output all points
				for (DataPoint r : pointList) {
					context.write(NullWritable.get(), new Text(r.getPointRecord().toString()));
				}
				return;
			}

			double radius = Math.pow((Math.pow(domainRange, num_dims) / searchList.size()), 1.0 / num_dims) / 5;
			while (searchList.size() > numOfPivots) {
				// System.out.println("Radius: " + radius);
				// for each point and radius r, count how many point is covered
				int maxNumCoverPoint = 0;
				ArrayList<Integer> maxCoverIndexInSearchingList = new ArrayList<Integer>();
				int iStarPointIndex = -1;

				for (int i = 0; i < pointList.size(); i++) {
					ArrayList<Integer> tempCoverIndexInSearching = new ArrayList<Integer>();
					for (Map.Entry<Integer, DataPoint> entry : searchList.entrySet()) {
						try {
							if (metric.dist(pointList.get(i).getPointRecord(),
									entry.getValue().getPointRecord()) < radius) {
								tempCoverIndexInSearching.add(entry.getKey());
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} // end for
					// int countCover = 0;
					// if (searchList.containsKey(((Record)
					// pointList.get(i).getPointRecord()).getRId()))
					// countCover = tempWeights - pointList.get(i).getWeight();
					// else
					// countCover = tempWeights;
					int countCover = Math.min(tempCoverIndexInSearching.size() - 1, searchList.size() - numOfPivots);
					if (countCover > maxNumCoverPoint) {
						maxNumCoverPoint = countCover;
						maxCoverIndexInSearchingList.clear();
						maxCoverIndexInSearchingList.addAll(tempCoverIndexInSearching);
						iStarPointIndex = i;
					}
				} // end for
				if (iStarPointIndex != -1) { // have clusters to merge
					// merge all clusters into one
					DataPoint newCenter = pointList.get(iStarPointIndex);
					HashMap<Integer, DataPoint> newPointList = new HashMap<Integer, DataPoint>();
					newPointList.putAll(newCenter.getCluster().getPointList());
					for (Integer pointId : maxCoverIndexInSearchingList) {
						// if (pointId.intValue() != ((Record)
						// newCenter.getPointRecord()).getRId()) {
						newPointList.putAll(searchList.get(pointId).getCluster().getPointList());
						// }
					}
					PointCluster newCluster = new PointCluster(newCenter, newPointList);
					// update all points in the cluster: point to the
					// newcluster,
					// and remove points from the searchList

					newCenter.setCluster(newCluster);
					for (DataPoint pointId : newPointList.values()) {
						pointId.setCluster(newCluster);
						searchList.remove(((Record) pointId.getPointRecord()).getRId());
					}
					// for (Integer pointId : maxCoverIndexInSearchingList) {
					// // searchList.get(pointId).setCluster(newCluster);
					// searchList.remove(pointId);
					// }
					searchList.put(((Record) newCenter.getPointRecord()).getRId(), newCenter);

					radius = Math.pow((Math.pow(domainRange, num_dims) / searchList.size()), 1.0 / num_dims) / 5;
				} else
					radius = radius * 2;
			}

			for (Map.Entry<Integer, DataPoint> entry : searchList.entrySet()) {
				context.write(NullWritable.get(),
						new Text(entry.getValue().getCluster().getCentralPoint().getPointRecord().toString()));
			} // end for
		}
	} // end reducer

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "K sum of diameter first round");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(KCoverSeperate.class);
		job.setMapperClass(KCoverSeperateMapper.class);
		job.setReducerClass(KCoverSeperateReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.kCovertempFile)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.kCovertempFile)));

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		KCoverSeperate kcover = new KCoverSeperate();
		kcover.run(args);
	}
}
