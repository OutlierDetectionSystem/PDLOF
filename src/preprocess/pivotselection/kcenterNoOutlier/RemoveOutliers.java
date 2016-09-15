package preprocess.pivotselection.kcenterNoOutlier;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.commons.math3.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

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


public class RemoveOutliers {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Dec 31, 2015
	 */

	public static class KCenterSeperateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
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

		/** The domains. (set by user) */
		private static double[][] domains;

		/** size of each partition */
		private static int smallRange;
		
		private static HashSet<Record> outliers;
		
		/** matrix space: text, vector */
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		
		private static int G;
		/**
		 * get MetricSpace and metric from configuration
		 * 
		 * @param conf
		 * @throws IOException
		 */
		private void readMetricAndMetricSpace(Configuration conf)
				throws IOException {
			try {
				metricSpace = MetricSpaceUtility.getMetricSpace(conf
						.get(SQConfig.strMetricSpace));
				metric = MetricSpaceUtility.getMetric(conf
						.get(SQConfig.strMetric));
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
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			domains = new double[num_dims][2];
			domains[0][0] = domains[1][0] = conf.getDouble(SQConfig.strDomainMin, 0.0);
			domains[0][1] = domains[1][1] = conf.getDouble(SQConfig.strDomainMax, 10001);
			partitionNumPerDim = conf.getInt(SQConfig.strNumOfPartitions, 201);
			smallRange = (int) Math.ceil((domains[0][1] - domains[0][0]) / partitionNumPerDim);
			outliers = new HashSet<Record>();
			readMetricAndMetricSpace(conf);
			G = conf.getInt(SQConfig.strDistOfG, 10);
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 1) {
					System.out.println("not enough cache files");
					return;
				}
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);

					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory()) {
							System.out.println("Reading outliers from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								Record newRecord = (Record) metricSpace.readObject(line, 2);
								outliers.add(newRecord);
							}
							currentReader.close();
							currentStream.close();
						}
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
		}

		/**
		 * Mapper -2 : Randomly select partitions for each point
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Text dat = new Text(value.toString());
			Record current = (Record) metricSpace.readObject(value.toString(), 2);
			boolean output = true;
			for(Record r: outliers){
				if(metric.dist(r, current) <= G*5){
					output=false;
					break;
				}
			}
			if(output)
				context.write(NullWritable.get(), dat);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "K center first round");
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(KCenterSeperate.class);
		job.setMapperClass(KCenterSeperateMapper.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.datasetNoOutliers)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.datasetNoOutliers)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strKCenterOutlierOuputPath)));
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		RemoveOutliers rescale = new RemoveOutliers();
		rescale.run(args);
	}
}
