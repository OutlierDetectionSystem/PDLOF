package triangulation;
//
//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;
//
//import triangulation.DelaunayTriangulator.Center;
//import weka.core.Instances;
//
//
//
public class LOFTest {
//
//	private static float sampleSize = 0.01f;
//
//	private static Random random = new Random();
//
//	private static float[] dataset;
//	private static float[] pivots;
//	private static float[] centroids;
//	private static float[] circumcenters;
//	private static int k;
//	private static int r;
//
//	/* usage: 
//	 * arg 1 csv file named dataset###.csv
//	 * arg 2 k 
//	 * arg 3 r
//	 * 
//	 * 	Generates:
//	 * 
//	 * randomly chosen initial pivot points
//	 * centroids computed from delaunay triangulation
//	 * circumcircles computed from triangulation
//	 * 
//	 * Computes LOF score for all data points
//	 * 
//	 * For top outliers, evaluates their average distance to pivots, centroids, circumcircles. 
//	 * 
//	 * output:
//	 * 
//	 * */
//	public static void main(String[] args) {
//
//		BufferedReader br = null;
//		BufferedWriter bw = null;
//
//		//int[] datasets = { 101, 170, 4 };
//		int[] datasets = {999};
//		for (int d : datasets) {
//
//			try {
//
//				// initialize files
//				FileReader inputFile = new FileReader("results/dataset" + d
//						+ ".csv");
//
//				File pivotFile = new File("results/pivots" + d + ".csv");
//				File centroidFile = new File("results/centroids" + d + ".csv");
//				File circumcenterFile = new File("results/circumcenters" + d
//						+ ".csv");
//
//				if (!pivotFile.exists()) {
//					pivotFile.createNewFile();
//				}
//				if (!centroidFile.exists()) {
//					centroidFile.createNewFile();
//				}
//				if (!circumcenterFile.exists()) {
//					circumcenterFile.createNewFile();
//				}
//
//	// find random pivots
//				System.out.println("Choosing pivots for dataset" + d);
//				getPivots(new BufferedReader(inputFile), new BufferedWriter(
//						new FileWriter(pivotFile.getAbsoluteFile())));
//
//	// perform delaunay triangulation using centroids
//				List<Double> centroidList = new DelaunayTriangulator()
//						.computeTriangles(new FloatArray(pivots), false, d,
//								Center.CENTROID);
//
//				bw = new BufferedWriter(new FileWriter(
//						centroidFile.getAbsoluteFile()));
//
//	// write center points to file, array
//				centroids = new float[centroidList.size()];
//				for (int i = 0; i < centroidList.size() - 1; i += 2) {
//					String line = centroidList.get(i) + ","+ centroidList.get(i + 1);
//					bw.write(line);
//					bw.write("\n");
//					centroids[i] = (float) (double) centroidList.get(i);
//					centroids[i+1] = (float) (double) centroidList.get(i+1);
//				}
//				bw.close();
//				System.out.println("centroids calculated for dataset" + d
//						+ "\n");
//
//				// perform delaunay triangulation using centroids
//
//				List<Double> circumcentersList = new DelaunayTriangulator()
//						.computeTriangles(new FloatArray(pivots), false, d,
//								Center.CIRCUMCIRCLE);
//
//				bw = new BufferedWriter(new FileWriter(
//						circumcenterFile.getAbsoluteFile()));
//
//				// write center points to file, array
//				circumcenters = new float[circumcentersList.size()];
//				for (int i = 0; i < circumcentersList.size() - 1; i += 2) {
//					bw.write(circumcentersList.get(i) + ","
//							+ circumcentersList.get(i + 1));
//					bw.write("\n");
//					circumcenters[i] = (float) (double) circumcentersList
//							.get(i);
//					circumcenters[i+1] = (float) (double) circumcentersList
//							.get(i+1);
//				}
//				bw.close();
//				System.out.println("circumcircles calculated for dataset" + d
//						+ "\n");
//
//				System.out.println("Performing  partitioning...");
//				
//				voronoi(new File("results/results" + d + ".csv"), dataset,
//						pivots, centroids, circumcenters);
//				
////calculate LOF scores using WEKA
//				
//				Instances data = new Instances(new BufferedReader(new FileReader("dataset999.arff")));
//				LOF lof = new LOF();
//				String[] options = {"-min 5", "-max 5"};
//				
//				weka.filters.Filter.runFilter(new LOF(), options);
//				
//				
//				//float[i] = neighbors for dataset[i]: n1, n2, ... , nk, k-dist
//				float[] neighborlist = new float[dataset.length/2];
//				
//				
//				for (int i = 0 ; i < dataset.length -1 ; i+=2){
//					float x = dataset[i];
//					float y = dataset[i+1];
//					float minDist = Float.MAX_VALUE;
//					float kDist = Float.MAX_VALUE;
//					
//				}
//				
//			}
//			
//			catch (IOException e) {
//				e.printStackTrace();
//			} finally {
//				try {
//					if (br != null)
//						br.close();
//					if (bw != null)
//						bw.close();
//				} catch (IOException ex) {
//					ex.printStackTrace();
//				}
//			}
//		}
//		
//
//		System.out.println("done");
//	}
//
//	/**
//	 * @param br
//	 *            reader for input dataset file.
//	 * @param bw
//	 *            writer for pivot point file.
//	 * @throws IOException
//	 * @throws NumberFormatException
//	 */
//	public static void getPivots(BufferedReader br, BufferedWriter bw)
//			throws IOException, NumberFormatException {
//
//		ArrayList<Float> points = new ArrayList<Float>();
//		ArrayList<Float> samplePoints = new ArrayList<Float>();
//
//		String currentLine;
//
//		// choose uniform random distribution of points and write to file
//		while ((currentLine = br.readLine()) != null) {
//			for (String s : currentLine.split(",")) {
//				points.add(Float.parseFloat(s));
//			}
//
//			float check = random.nextFloat();
//			if (check <= sampleSize) {
//				bw.write(currentLine);
//				bw.write("\n");
//				for (String s : currentLine.split(",")) {
//					samplePoints.add(Float.parseFloat(s));
//				}
//			}
//		}
//		br.close();
//		bw.close();
//
//		dataset = new float[points.size()];
//		for (int i = 0; i < points.size(); i++) {
//			dataset[i] = points.get(i);
//		}
//
//		pivots = new float[samplePoints.size()];
//		for (int i = 0; i < samplePoints.size(); i++) {
//			pivots[i] = samplePoints.get(i);
//		}
//
//		System.out.println("dataset size = " + points.size()/2);
//		System.out.println("sample size = " + samplePoints.size()/2);
//
//	}
//
//	/**
//	 * Writes voronoi partitioning to file in format x, y, initial_partition_num,
//	 * centroid_partition_num, circumcircle_partition_num.
//	 * 
//	 * @param d
//	 *            the dataset id
//	 * @param dataset
//	 * @param pivots
//	 * @param centroids
//	 * @param circumcenters
//	 * @throws IOException
//	 * @throws NumberFormatException
//	 */
//	public static void voronoi(File outputFile, float[] dataset,
//			float[] pivots, float[] centroids, float[] circumcenters)
//			throws IOException, NumberFormatException {
//
//		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
//		StringBuffer buff = null;
//		for (int i = 0; i < dataset.length - 1; i += 2) {
//
//			float x = dataset[i];
//			float y = dataset[i + 1];
//
//			buff = new StringBuffer();
//			buff.append(x);
//			buff.append(",");
//			buff.append(y);
//
//			// perform voronoi partitioning on initial pivots
//			int index = getClosestPivot(pivots, x, y);
//			buff.append(",");
//			buff.append(index);
//
//			// perform voronoi partitioning using centroids
//			index = getClosestPivot(centroids, x, y);
//			buff.append(",");
//			buff.append(index);
//
//			// perform voronoi partitioning using circumcenters
//			index = getClosestPivot(circumcenters, x, y);
//			buff.append(",");
//			buff.append(index);
//
//			// write the values for this point to file
//			bw.write(buff.toString());
//			bw.write("\n");
//		}
//		bw.close();
//	}
//
//	/**
//	 * returns the index of the x coordinate of pivot, used to number pivots.
//	 * @param pivots
//	 * @param x
//	 * @param y
//	 * @return
//	 */
//	public static int getClosestPivot(float[] pivots, float x, float y) {
//		float minDist = Float.MAX_VALUE;
//		int pivot = -1;
//		for (int j = 0; j < pivots.length - 1; j += 2) {
//			float dist = dist(x, y, pivots[j], pivots[j + 1]);
//			if (dist < minDist) {
//				minDist = dist;
//				pivot = j;
//			}
//		}
//		return pivot;
//	}
//
//	/**
//	 * 
//	 * Compute distance between two 2d points.
//	 * 
//	 * @param x1
//	 * @param x2
//	 * @param y1
//	 * @param y2
//	 * @return
//	 */
//	static float dist(float x1, float y1, float x2, float y2) {
//		return (float) Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
//	}
//
//	// test on small dataset0
//	public void smallTest() {
//
//		File pivotFile = new File("results/pivots0.csv");
//		File centerFile = new File("results/centers0.csv");
//		BufferedReader br = null;
//		BufferedWriter bw = null;
//		String currentLine;
//		ArrayList<Float> samplePoints = new ArrayList<Float>();
//
//		try {
//			// read pivots from file
//			br = new BufferedReader(new FileReader(pivotFile));
//			while ((currentLine = br.readLine()) != null) {
//				for (String s : currentLine.split(",")) {
//					samplePoints.add(Float.parseFloat(s));
//				}
//			}
//
//			// perform delaunay triangulation, retrieve center points
//			float[] farray = new float[samplePoints.size()];
//			for (int i = 0; i < samplePoints.size(); i++) {
//				farray[i] = samplePoints.get(i);
//			}
//
//			List<Double> centerList = new DelaunayTriangulator()
//					.computeTriangles(new FloatArray(farray), false, 0,
//							Center.CENTROID);
//
//			bw = new BufferedWriter(
//					new FileWriter(centerFile.getAbsoluteFile()));
//
//			// write center points to file
//			for (int i = 0; i < centerList.size() - 1; i += 2) {
//				bw.write(centerList.get(i) + "," + centerList.get(i+1));
//				bw.write("\n");
//			}
//			System.out.println("center points calculated for dataset0");
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (br != null)
//					br.close();
//				if (bw != null)
//					bw.close();
//			} catch (IOException ex) {
//				ex.printStackTrace();
//			}
//		}
//
//	}
//
//	// triangulation test on four points
//	public void tinyTest() {
//		// test triangulation on small set
//		float[] test = { 10.0f, 10.0f, 30.0f, 40.0f, 40.0f, 0.0f, 5.0f, 50.0f };
//		List<Double> centerList = new DelaunayTriangulator()
//				.computeTriangles(new FloatArray(test), false, 111,
//						Center.CENTROID);
//	}
}
