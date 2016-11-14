package preprocess.pivotselection.kcover.iteration;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import preprocess.pivotselection.SQConfig;
import preprocess.pivotselection.kcover.iteration.KCoverSeperateWithKDTree.KCoverSeperateMapper;
import preprocess.pivotselection.kcover.iteration.KCoverSeperateWithKDTree.KCoverSeperateReducer;
import preprocess.pivotselection.kcover.iteration.KCoverSeperateWithKDTree.KCoverFinalRoundMapper;
import preprocess.pivotselection.kcover.iteration.KCoverSeperateWithKDTree.KCoverFirstMapper;


public class KCoverDriver {
	/**
	 * k-center algorithm program
	 */
	private static String temp_path = "/yyan/kcenter/temp_center/";
	private static String dataPath;

	public static long currentTotalPoint = 30000000;
	public static int pointPerPartition = 10000;
	public static int currentNumPartitions = 100;
	public static int pivotCount = 500;
	public static int iterNum = 0;

	private static Log log = LogFactory.getLog(KCoverDriver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "kcover job" + " " + iterNum);
		String strFSName = conf.get("fs.default.name");
		// set the centers data file
		job.setJarByClass(KCoverDriver.class);
		job.setMapperClass(KCoverFirstMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		job.setReducerClass(KCoverSeperateReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
//		dataPath = conf.get(SQConfig.dataset);
		dataPath = "/lof/datasets/start";
		FileInputFormat.addInputPath(job, new Path(strFSName+dataPath));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(temp_path), true);
		currentTotalPoint = conf.getLong(SQConfig.strDatasetSize, 30000000);
		pointPerPartition = conf.getInt(SQConfig.strNumOfPartitions, 10000);
		currentNumPartitions = (int) (currentTotalPoint / pointPerPartition);
		pivotCount = conf.getInt(SQConfig.strNumOfPivots, 500);
		System.out.println("Total Point: " + currentTotalPoint);
		System.out.println("Point per partition: " + pointPerPartition);
		System.out.println("Current Number of partition: " + currentNumPartitions);
		FileOutputFormat.setOutputPath(job, new Path(temp_path + 0 + "/"));
		if (!job.waitForCompletion(true)) {
			System.exit(1); // run error then exit
		}
		currentTotalPoint = currentNumPartitions * pivotCount;
		currentNumPartitions = (int) (currentTotalPoint / pointPerPartition);
		// do iteration
		iterNum++;
		boolean flag = true;
		while (flag && currentTotalPoint > 1.5 * pointPerPartition) {
			Configuration conf1 = new Configuration();
			conf1.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
			conf1.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
			new GenericOptionsParser(conf1, args).getRemainingArgs();
			System.out.println("Total Point: " + currentTotalPoint);
			System.out.println("Point per partition: " + pointPerPartition);
			System.out.println("Current Number of partition: " + currentNumPartitions);
			boolean iterflag = doIteration(conf1, iterNum);
			if (!iterflag) {
				log.error("job fails");
				System.exit(1);
			}
			currentTotalPoint = currentNumPartitions * pivotCount;
			currentNumPartitions = (int) (currentTotalPoint / pointPerPartition);
			iterNum++;
		}
		System.out.println(iterNum);
		
		// the last job , classify the data

		Configuration conf2 = new Configuration();
		conf2.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf2.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf2, args).getRemainingArgs();
		// set the centers data file
		lastJob(conf2, iterNum);
		
	}

	public static boolean doIteration(Configuration conf, int iterNum)
			throws IOException, ClassNotFoundException, InterruptedException {
		boolean flag = false;

		Job job = Job.getInstance(conf, "kcover job" + " " + iterNum);
		// set the centers data file
		Path centersFile1 = new Path(temp_path + (iterNum - 1));
		String strFSName = conf.get("fs.default.name");

		job.setJarByClass(KCoverDriver.class);
		job.setMapperClass(KCoverSeperateMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		job.setReducerClass(KCoverSeperateReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		// FileInputFormat.addInputPath(job, new
		// Path(conf.get(SQConfig.dataset)));
		FileInputFormat.addInputPath(job, centersFile1);
		FileOutputFormat.setOutputPath(job, new Path(temp_path + iterNum + "/"));
		flag = job.waitForCompletion(true);
		return flag;
	}
	
	public static void lastJob(Configuration conf, int iterNum)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "kcover job" + " " + iterNum);
		
		job.setJarByClass(KCoverDriver.class);
		job.setMapperClass(KCoverFinalRoundMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(KCoverSeperateReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		Path centersFile1 = new Path(temp_path + (iterNum - 1));
		FileInputFormat.addInputPath(job, centersFile1);
		FileSystem fs = FileSystem.get(conf);
//		System.out.println("Final input:" + temp_path+(iterNum-1));
		
		fs.delete(new Path(conf.get(SQConfig.kCoverFinalOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.kCoverFinalOutput)));
		job.waitForCompletion(true);
	}


}
