package preprocess.pivotselection.kmean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;  
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import preprocess.pivotselection.SQConfig;



public class KmeansDriver {

	/**
	 * k-means algorithm program
	 */
	private static final String temp_path = "/yyan/kmeans/temp_center/";
	private static final String dataPath = "/yyan/input/smallkmeansdata/";
	private static final int iterTime = 20;
	private static int iterNum = 1;
	private static final double threadHold = 0.01;

	private static Log log = LogFactory.getLog(KmeansDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		try {
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
			conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
			new GenericOptionsParser(conf, args).getRemainingArgs();
			
			Job job = Job.getInstance(conf, "kmeans job" + " " + iterNum);
			String strFSName = conf.get("fs.default.name");
			// set the centers data file
			Path centersFile = new Path("/yyan/input/centers");
			job.addCacheArchive(new URI(strFSName + centersFile));
			job.setJarByClass(KmeansDriver.class);
			job.setMapperClass(KmeansM.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(DataPro.class);
			job.setNumReduceTasks(1);
			job.setCombinerClass(KmeansC.class);
			job.setReducerClass(KmeansR.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
//			FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
			FileInputFormat.addInputPath(job, new Path(dataPath));
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(temp_path), true);
			FileOutputFormat.setOutputPath(job, new Path(temp_path + 0 + "/"));
			if (!job.waitForCompletion(true)) {
				System.exit(1); // run error then exit
			}
			
			// do iteration
			boolean flag = true;
			while (flag && iterNum < iterTime) {
				Configuration conf1 = new Configuration();
				conf1.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
				conf1.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
				new GenericOptionsParser(conf1, args).getRemainingArgs();

				boolean iterflag = doIteration(conf1, iterNum);
				if (!iterflag) {
					log.error("job fails");
					System.exit(1);
				}
				// set the flag based on the old centers and the new centers

				Path oldCentersFile = new Path(temp_path + (iterNum - 1) + "/part-r-00000");
				Path newCentersFile = new Path(temp_path + iterNum + "/part-r-00000");
				FileSystem fs1 = FileSystem.get(oldCentersFile.toUri(), conf1);
				FileSystem fs2 = FileSystem.get(oldCentersFile.toUri(), conf1);
				if (!(fs1.exists(oldCentersFile) && fs2.exists(newCentersFile))) {
					log.info("the old centers and new centers should exist at the same time");
					System.exit(1);
				}
				String line1, line2;
				FSDataInputStream in1 = fs1.open(oldCentersFile);
				FSDataInputStream in2 = fs2.open(newCentersFile);
				InputStreamReader istr1 = new InputStreamReader(in1);
				InputStreamReader istr2 = new InputStreamReader(in2);
				BufferedReader br1 = new BufferedReader(istr1);
				BufferedReader br2 = new BufferedReader(istr2);
				double error = 0.0;
				while ((line1 = br1.readLine()) != null && ((line2 = br2.readLine()) != null)) {
					String[] str1 = line1.split(",");
					String[] str2 = line2.split(",");
					for (int i = 0; i < str1.length; i++) {
						error += (Double.parseDouble(str1[i]) - Double.parseDouble(str2[i]))
								* (Double.parseDouble(str1[i]) - Double.parseDouble(str2[i]));
					}
				}
				if (error < threadHold) {
					flag = false;
				}
				iterNum++;

			}
			// the last job , classify the data

			// Configuration conf2 = new Configuration();
			// conf2.addResource(new
			// Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
			// conf2.addResource(new
			// Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
			// // set the centers data file
			// Path centersFile2 = new Path(temp_path + (iterNum - 1) +
			// "/part-r-00000");
			// job.addCacheArchive(new URI(strFSName + centersFile2));
			//
			// lastJob(conf2, iterNum);
			System.out.println(iterNum);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static boolean doIteration(Configuration conf, int iterNum)
			throws IOException, ClassNotFoundException, InterruptedException {
		boolean flag = false;
		
		Job job = Job.getInstance(conf, "kmeans job" + " " + iterNum);
		// set the centers data file
		Path centersFile1 = new Path(temp_path + (iterNum - 1) + "/part-r-00000");
		String strFSName = conf.get("fs.default.name");
		try {
			job.addCacheArchive(new URI(strFSName + centersFile1));
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job.setJarByClass(KmeansDriver.class);
		job.setMapperClass(KmeansM.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DataPro.class);
		job.setNumReduceTasks(1);
		job.setCombinerClass(KmeansC.class);
		job.setReducerClass(KmeansR.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileInputFormat.addInputPath(job, new Path(dataPath));
		FileOutputFormat.setOutputPath(job, new Path(temp_path + iterNum + "/"));
		flag = job.waitForCompletion(true);
		return flag;
	}

//	public static void lastJob(Configuration conf, int iterNum)
//			throws IOException, ClassNotFoundException, InterruptedException {
//		Job job = Job.getInstance(conf, "kmeans job" + " " + iterNum);
//		job.setJarByClass(KmeansDriver.class);
//		job.setMapperClass(KmeansLastM.class);
//		job.setMapOutputKeyClass(IntWritable.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setNumReduceTasks(4);
//		// job.setCombinerClass(KmeansC.class);
//		job.setReducerClass(KmeansLastR.class);
//		job.setOutputKeyClass(IntWritable.class);
//		job.setOutputValueClass(Text.class);
//		FileInputFormat.addInputPath(job, new Path(dataPath));
//		FileOutputFormat.setOutputPath(job, new Path(temp_path + iterNum + "/"));
//		job.waitForCompletion(true);
//	}

}