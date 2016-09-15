package preprocess.pivotselection;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricFactory;
import metricspace.MetricSpaceFactory;
import preprocess.pivotselection.APivotSelection;
/**
 * We use it to select pivots according to the selection strategies: 
 * (1) random (2) farthest (3) greedy
 * 
 * @author Yizhou Yan
 *
 */
public class PivotSelectionFactory extends APivotSelection {
	int sampleSize = 10000;
	Object[] sample = null;
	int dim;
	int numOfPivots;
	int numOfPivotSets = 1;
	IMetricSpace metricSpace;

	/**
	 * 
	 * @param ms
	 *            : metric space to be operated
	 * @param pivot
	 *            : number of pivots
	 * @param dim
	 *            : dimension; set to 1 for text space
	 */
	public PivotSelectionFactory(IMetricSpace ms, int numOfPivots, int dim) {
		metricSpace = ms;
		this.numOfPivots = numOfPivots;
		this.dim = dim;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			long begin = System.currentTimeMillis();
			pivotSelectionMain(args);
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("PivotSelection takes " + second + " seconds");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * We read lines from the source files according to the line ids that are
	 * maintained in Map readLineIDs with format <lineId, (pivotSetID, offset)>
	 * 
	 * @param path
	 *            : source file path dir
	 * @param readLineIDs
	 *            : line ids to be read
	 */
	private void read(String input, Map<Integer, Long> readLineIDs)
			throws IOException {
		if (readLineIDs == null || readLineIDs.size() != sampleSize)
			return;

		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inputStream = fs.open(new Path(input));
		BufferedReader br;
		br = new BufferedReader(new InputStreamReader(inputStream));
		String tmp;
		int lineIDOfFile = 0;
		int offsetOfSample;

		while ((tmp = br.readLine()) != null) {
			if (readLineIDs.containsKey(lineIDOfFile)) {
				long value = readLineIDs.get(lineIDOfFile);
				int offset = (int) (value & 0x00000000ffffffff);
				int pivotSetID = (int) (value >> 32);

				offsetOfSample = pivotSetID * numOfPivots + offset;
				sample[offsetOfSample] = metricSpace.readObject(tmp, dim);
			}
			lineIDOfFile++;
		}

		br.close();
		inputStream.close();
		fs.close();
	}

	@Override
	public int read(String dir, int numOfObjects, int totNumOfObjects) {
		/**
		 * ======begin to generate a set of random ids as the
		 * samples================
		 */
		sampleSize = numOfObjects;
		sample = new Object[sampleSize];
		numOfPivotSets = numOfObjects / numOfPivots;

		int i, j;
		int lineID;
		long value;
		/**
		 * Map randLineIDs is used to map line ids to position in the sampling
		 * array
		 */
		Map<Integer, Long> randLineIDs = new HashMap<Integer, Long>(sampleSize);
		Random randomGenerator = new Random(numOfObjects);

		for (i = 0; i < numOfPivotSets; i++) {
			for (j = 0; j < numOfPivots; j++) {
				while (true) {
					/** duplicates are not allowed in our case */
					lineID = randomGenerator.nextInt(totNumOfObjects+1);
					if (!randLineIDs.containsKey(lineID)) {
						value = i;
						value = (value << 32) + j;
						randLineIDs.put(lineID, value);
						break;
					}
				}
			}
		}
		/**
		 * ======end to generate a set of random ids as the
		 * samples================
		 */
		try {
			read(dir, randLineIDs);
			return 1;
		} catch (Exception ex) {
			ex.printStackTrace();
			return -1;
		}
	}

	@Override
	public void selectPivots(SelStrategy sg, IMetric metric) throws IOException {
		if (sg == SelStrategy.random) {
			randomSelect(metric);
		}else {
			System.out.println("Error: We do not support " + sg + " yet!");
		}
		System.out.println("===========pivots selection ends.=============");
	}

	private double getSumDistFromPivotSet(IMetric metric, int offset)
			throws IOException {
		double sum = 0.0d;

		if (offset >= this.sampleSize)
			return sum;

		for (int i = offset; i < offset + numOfPivots - 1; i++) {
			for (int j = i + 1; j < offset + numOfPivots; j++) {
				sum += metric.dist(sample[i], sample[j]);
			}
		}
		return sum;
	}

	/**
	 * For each pivot set, we compute the distance between every two pivots; The
	 * pivot set with maximum sum distance is taken as the final pivot set.
	 * 
	 * Note that the selected pivot is set to first set in the sample
	 * 
	 * @param metric
	 * @throws Exception
	 */
	private void randomSelect(IMetric metric) throws IOException {
		int pSetID = 0, i;
		double sum, tmp;

		sum = getSumDistFromPivotSet(metric, 0);
		for (i = 1; i < numOfPivotSets; i++) {
			tmp = getSumDistFromPivotSet(metric, numOfPivots * i);
			if (tmp > sum) {
				sum = tmp;
				pSetID = i;
			}
		}

		if (pSetID != 0) {
			// move the selected pivot set to the first set
			for (i = 0; i < numOfPivots; i++) {
				sample[i] = sample[pSetID * numOfPivots + i];
			}
		}
	}

	@Override
	public void output(String path) {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream outputStream = fs.create(new Path(path), true);
			BufferedWriter br = null;
			br = new BufferedWriter(new OutputStreamWriter(outputStream));

			for (int i = 0; i < numOfPivots; i++) {
				// we just add an invalid id for each pivot for the consistency
				String line = i + "," + metricSpace.outputDim(sample[i]);
				br.write(line + "\n");
			}

			br.close();
			outputStream.close();
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * select pivots from the data set
	 * 
	 * @param args
	 *            :
	 */
	public static void pivotSelectionMain(String[] args)
			throws ClassNotFoundException, IOException {
		String strDomain;
		String strMetric;
		String inputPath;
		String outputPath;
		String strStrategy;
		int numOfPivots;
		int sampleSize;
		int totSize;
		int dim;
		// get configurations 
		Configuration conf = new Configuration();
		new GenericOptionsParser(conf, args).getRemainingArgs();

		inputPath = conf.get(SQConfig.dataset) + Path.SEPARATOR + "0.data";
		outputPath = conf.get(SQConfig.strPivotInput);
		strDomain = conf.get(SQConfig.strMetricSpace);
		strMetric = conf.get(SQConfig.strMetric);
		strStrategy = conf.get(SQConfig.strSelStrategy);
		numOfPivots = conf.getInt(SQConfig.strNumOfPivots, -1);
		sampleSize = conf.getInt(SQConfig.strSampleSize, -1);
		totSize = conf.getInt(SQConfig.strDatasetSize, -1);
		dim = conf.getInt(SQConfig.strDimExpression, -1);

		IMetricSpace metricSpace = null;
		IMetric metric = null;

		if (strMetric.compareToIgnoreCase("L1Metric") == 0) {
			metric = new MetricFactory.L1Metric();
		} else if (strMetric.compareToIgnoreCase("L2Metric") == 0) {
			metric = new MetricFactory.L2Metric();
		} else {
			throw new ClassNotFoundException("The input metric " + strMetric
					+ " is not yet supported!");
		}

		if (strDomain.compareTo("vector") == 0) {
			metricSpace = new MetricSpaceFactory.VectorSpace(metric);
		} else {
			throw new ClassNotFoundException("The input metricspace "
					+ strDomain + "is not yet supported!");
		}

		System.err.println("inputPath: " + inputPath + "\noutputPath: "
				+ outputPath + "\nDomain: " + strDomain + ", metric: "
				+ strMetric + ", NumOfPivots: " + numOfPivots
				+ ", NumOfSample: " + sampleSize + ", TotSize: " + totSize);

		// make sure that the sample size is the multiple times to numOfPivots
		if (sampleSize % numOfPivots != 0) {
			throw new IOException(
					"The sizeOfSample must be set to the multiple times of numOfPivots");
		}

		// read, select, and output
		APivotSelection ps = new PivotSelectionFactory(metricSpace,
				numOfPivots, dim);
		ps.read(inputPath, sampleSize, totSize);
		APivotSelection.SelStrategy ss;
		if (strStrategy.compareToIgnoreCase("random") == 0) {
			ss = APivotSelection.SelStrategy.random;
			ps.selectPivots(ss, metric);
		}
		else {
			throw new ClassNotFoundException("The input selection strategy "
					+ strStrategy + "is not yet supported!");
		}
		ps.output(outputPath);
	}
}
