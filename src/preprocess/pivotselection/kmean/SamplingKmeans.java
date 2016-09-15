package preprocess.pivotselection.kmean;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import preprocess.pivotselection.SQConfig;


public class SamplingKmeans {
	public static class SamplingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		/** incrementing index of divisions generated in mapper */
		public static int increIndex = 0;

		/** number of divisions where data is divided into (set by user) */
		private int denominator = 100;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int id = increIndex % denominator;
			increIndex++;
			
			Text dat = new Text(value.toString());
			if (id == 0) {
				context.write(NullWritable.get(), dat);
			}
		}
	}
	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "Sampling for kmeans");

		job.setJarByClass(SamplingKmeans.class);
		job.setMapperClass(SamplingMapper.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);

		String strFSName  = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/yyan/input/smallkmeansdata/"), true);
		FileOutputFormat.setOutputPath(job, new Path("/yyan/input/smallkmeansdata/"));

		// job.addCacheFile(new URI(strFSName +
		// conf.get(SQConfig.strPivotInput)));
		// job.addCacheFile(new URI(strFSName +
		// conf.get(SQConfig.strMergeIndexOutput)
		// + Path.SEPARATOR + "summary" + SQConfig.strIndexExpression1));
		
		/** print job parameter */
		System.err.println("# of dim: "
				+ conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		SamplingKmeans skmeans = new SamplingKmeans();
		skmeans.run(args);
	}

}
