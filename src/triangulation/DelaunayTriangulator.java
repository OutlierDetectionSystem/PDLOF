package triangulation;

import java.awt.geom.Point2D;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Delaunay triangulation. Adapted from Paul Bourke's triangulate:
 * http://paulbourke.net/papers/triangulate/
 * 
 * @author Nathan Sweet
 */
public class DelaunayTriangulator {

	public enum Center {
		CENTROID, CIRCUMCIRCLE;
	}

	static private final float EPSILON = 0.000001f;
	static private final int INSIDE = 0;
	static private final int COMPLETE = 1;
	static private final int INCOMPLETE = 2;

	private final IntArray quicksortStack = new IntArray();
	private final IntArray triangles = new IntArray(false, 16);
	private final IntArray edges = new IntArray();
	private final BooleanArray complete = new BooleanArray(false, 16);
	private final float[] superTriangle = new float[6];

	// dataset label used for writing triangles file
	private String outputFile;

	/**
	 * @param d
	 * @see #computeTriangles(float[], int, int, boolean)
	 */
	public ArrayList<Double> computeTriangles(FloatArray points,
			boolean sorted, int dataset, Center center) {

		// set file path to write triangles
		if (center == Center.CENTROID) {
			this.outputFile = "results/centroid_triangles" + dataset + ".csv";
		} else if (center == Center.CIRCUMCIRCLE) {
			this.outputFile = "results/circumcircle_triangles" + dataset
					+ ".csv";
		}
		return computeTriangles(points.items, 0, points.size, sorted, center);
	}

	/** @see #computeTriangles(float[], int, int, boolean) */
	public ArrayList<Double> computeTriangles(float[] polygon,
			boolean sorted, Center center) {
		return computeTriangles(polygon, 0, polygon.length, sorted, center);
	}

	/**
	 * Triangulates the given point cloud to a list of triangle indices that
	 * make up the Delaunay triangulation.
	 * 
	 * @param points
	 *            x,y pairs describing points. Duplicate points will result in
	 *            undefined behavior.
	 * @param sorted
	 *            If false, the points will be sorted by the x coordinate, which
	 *            is required by the triangulation algorithm.
	 * @return triples of indexes into the points that describe the triangles in
	 *         clockwise order. Note the returned array is reused for later
	 *         calls to the same method.
	 */
	public ArrayList<Double> computeTriangles(float[] points, int offset,
			int count, boolean sorted, Center center) {
		int end = offset + count;

		if (!sorted)
			quicksortPairs(points, offset, end - 1);

		// Determine bounds for super triangle.
		float xmin = points[0], ymin = points[1];
		float xmax = xmin, ymax = ymin;
		for (int i = offset + 2; i < end; i++) {
			float value = points[i];
			if (value < xmin)
				xmin = value;
			if (value > xmax)
				xmax = value;
			i++;
			value = points[i];
			if (value < ymin)
				ymin = value;
			if (value > ymax)
				ymax = value;
		}
		float dx = xmax - xmin, dy = ymax - ymin;
		float dmax = (dx > dy ? dx : dy) * 20f;
		float xmid = (xmax + xmin) / 2f, ymid = (ymax + ymin) / 2f;

		// Setup the super triangle, which contains all points.
		float[] superTriangle = this.superTriangle;
		superTriangle[0] = xmid - dmax;
		superTriangle[1] = ymid - dmax;
		superTriangle[2] = xmid;
		superTriangle[3] = ymid + dmax;
		superTriangle[4] = xmid + dmax;
		superTriangle[5] = ymid - dmax;

		IntArray edges = this.edges;
		edges.ensureCapacity(count / 2);

		BooleanArray complete = this.complete;
		complete.ensureCapacity(count);

		IntArray triangles = this.triangles;
		triangles.clear();
		triangles.ensureCapacity(count);

		// Add super triangle.
		triangles.add(end);
		triangles.add(end + 2);
		triangles.add(end + 4);
		complete.add(false);

		// Include each point one at a time into the existing mesh.
		for (int pointIndex = offset; pointIndex < end; pointIndex += 2) {
			float x = points[pointIndex], y = points[pointIndex + 1];

			// If x,y lies inside the circumcircle of a triangle, the edges are
			// stored and the triangle removed.
			int[] trianglesArray = triangles.items;
			boolean[] completeArray = complete.items;
			for (int triangleIndex = triangles.size - 1; triangleIndex >= 0; triangleIndex -= 3) {
				int completeIndex = triangleIndex / 3;
				if (completeArray[completeIndex])
					continue;
				int p1 = trianglesArray[triangleIndex - 2];
				int p2 = trianglesArray[triangleIndex - 1];
				int p3 = trianglesArray[triangleIndex];
				float x1, y1, x2, y2, x3, y3;
				if (p1 >= end) {
					int i = p1 - end;
					x1 = superTriangle[i];
					y1 = superTriangle[i + 1];
				} else {
					x1 = points[p1];
					y1 = points[p1 + 1];
				}
				if (p2 >= end) {
					int i = p2 - end;
					x2 = superTriangle[i];
					y2 = superTriangle[i + 1];
				} else {
					x2 = points[p2];
					y2 = points[p2 + 1];
				}
				if (p3 >= end) {
					int i = p3 - end;
					x3 = superTriangle[i];
					y3 = superTriangle[i + 1];
				} else {
					x3 = points[p3];
					y3 = points[p3 + 1];
				}
				switch (circumCircle(x, y, x1, y1, x2, y2, x3, y3)) {
				case COMPLETE:
					completeArray[completeIndex] = true;
					break;
				case INSIDE:
					edges.add(p1);
					edges.add(p2);
					edges.add(p2);
					edges.add(p3);
					edges.add(p3);
					edges.add(p1);

					triangles.removeIndex(triangleIndex);
					triangles.removeIndex(triangleIndex - 1);
					triangles.removeIndex(triangleIndex - 2);
					complete.removeIndex(completeIndex);
					break;
				}
			}

			int[] edgesArray = edges.items;
			for (int i = 0, n = edges.size; i < n; i += 2) {
				// Skip multiple edges. If all triangles are anticlockwise then
				// all interior edges are opposite pointing in direction.
				int p1 = edgesArray[i];
				if (p1 == -1)
					continue;
				int p2 = edgesArray[i + 1];
				boolean skip = false;
				for (int ii = i + 2; ii < n; ii += 2) {
					if (p1 == edgesArray[ii + 1] && p2 == edgesArray[ii]) {
						skip = true;
						edgesArray[ii] = -1;
					}
				}
				if (skip)
					continue;

				// Form new triangles for the current point. Edges are arranged
				// in clockwise order.
				triangles.add(p1);
				triangles.add(edgesArray[i + 1]);
				triangles.add(pointIndex);
				complete.add(false);
			}
			edges.clear();
		}
		complete.clear();

		// Remove triangles with super triangle vertices.
		int[] trianglesArray = triangles.items;
		for (int i = triangles.size - 1; i >= 0; i -= 3) {
			if (trianglesArray[i] >= end || trianglesArray[i - 1] >= end
					|| trianglesArray[i - 2] >= end) {
				triangles.removeIndex(i);
				triangles.removeIndex(i - 1);
				triangles.removeIndex(i - 2);
			}
		}

		int[] triPoints = triangles.toArray();
		ArrayList<Double> centerList = new ArrayList<Double>();
		Double[] c = new Double[2];

		// CK - find circumcircle of each triangle, return centers
		for (int i = 0; i < triPoints.length; i += 3) {
			float x = points[triPoints[i]];
			float y = points[triPoints[i] + 1];

			float x1 = points[triPoints[i + 1]];
			float y1 = points[triPoints[i + 1] + 1];

			float x2 = points[triPoints[i + 2]];
			float y2 = points[triPoints[i + 2] + 1];

			if (center == Center.CIRCUMCIRCLE) {
				c =(circumCenter(x, y, x1, y1, x2, y2));
			} else if (center == Center.CENTROID) {
				c = (centroid(x, y, x1, y1, x2, y2));
			}
			centerList.add(c[0]);
			centerList.add(c[1]);
		}

		// print triangles to file
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(new File(outputFile)));

			for (int i = 0; i < triPoints.length; i += 3) {
				bw.write(points[triPoints[i]] + ", " + points[triPoints[i] + 1]);
				bw.write("\n");
				bw.write(points[triPoints[i + 1]] + ", "
						+ points[triPoints[i + 1] + 1]);
				bw.write("\n");
				bw.write(points[triPoints[i + 2]] + ", "
						+ points[triPoints[i + 2] + 1]);
				bw.write("\n");

			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		return centerList;
	}

	/**
	 * Returns center point of the circumcircle of triangle.
	 * 
	 * @param x
	 *            y points of triangle.
	 */
	private Double[] circumCenter(float x0, float y0, float x1, float y1,
			float x2, float y2) {

		Point2D.Double midDiff = new Point2D.Double((x2 - x0) / 2,
				(y2 - y0) / 2);
		Point2D.Double u = new Point2D.Double(y0 - y1, x1 - x0);
		Point2D.Double v = new Point2D.Double(x2 - x1, y2 - x1);
		double t = dot(midDiff, v) / dot(u, v);
		Point2D.Double circumcenter = new Point2D.Double(t * u.x + (x0 + x1)
				/ 2, t * u.y + (y0 + y1) / 2);
		Double[] point = new Double[2];
		point[0] = circumcenter.x;
		point[1] = circumcenter.y;
		return point;
	}

	/**
	 * Returns centroid of triangle.
	 * 
	 * @param x
	 *            y points of triangle.
	 */
	private Double[] centroid(float x0, float y0, float x1, float y1, float x2,
			float y2) {

		Double[] point = new Double[2];
		point[0] = (x0 + x1 + x2) / 3.0;
		point[1] = (y0 + y1 + y2) / 3.0;

		return point;
	}

	double dot(Point2D.Double q0, Point2D.Double q1) {
		return q0.x * q1.x + q0.y * q1.y;
	}

	/**
	 * Returns INSIDE if point xp,yp is inside the circumcircle made up of the
	 * points x1,y1, x2,y2, x3,y3. Returns COMPLETE if xp is to the right of the
	 * entire circumcircle. Otherwise returns INCOMPLETE. Note: a point on the
	 * circumcircle edge is considered inside.
	 */
	private int circumCircle(float xp, float yp, float x1, float y1, float x2,
			float y2, float x3, float y3) {
		float xc, yc;
		float y1y2 = Math.abs(y1 - y2);
		float y2y3 = Math.abs(y2 - y3);
		if (y1y2 < EPSILON) {
			if (y2y3 < EPSILON)
				return INCOMPLETE;
			float m2 = -(x3 - x2) / (y3 - y2);
			float mx2 = (x2 + x3) / 2f;
			float my2 = (y2 + y3) / 2f;
			xc = (x2 + x1) / 2f;
			yc = m2 * (xc - mx2) + my2;
		} else {
			float m1 = -(x2 - x1) / (y2 - y1);
			float mx1 = (x1 + x2) / 2f;
			float my1 = (y1 + y2) / 2f;
			if (y2y3 < EPSILON) {
				xc = (x3 + x2) / 2f;
				yc = m1 * (xc - mx1) + my1;
			} else {
				float m2 = -(x3 - x2) / (y3 - y2);
				float mx2 = (x2 + x3) / 2f;
				float my2 = (y2 + y3) / 2f;
				xc = (m1 * mx1 - m2 * mx2 + my2 - my1) / (m1 - m2);
				yc = m1 * (xc - mx1) + my1;
			}
		}

		float dx = x2 - xc;
		float dy = y2 - yc;
		float rsqr = dx * dx + dy * dy;

		dx = xp - xc;
		dx *= dx;
		dy = yp - yc;
		if (dx + dy * dy - rsqr <= EPSILON)
			return INSIDE;
		return xp > xc && dx > rsqr ? COMPLETE : INCOMPLETE;
	}

	/**
	 * Sorts x,y pairs of values by the x value.
	 * 
	 * @param lower
	 *            Start x index.
	 * @param upper
	 *            End x index.
	 */
	private void quicksortPairs(float[] values, int lower, int upper) {
		IntArray stack = quicksortStack;
		stack.add(lower);
		stack.add(upper - 1);
		while (stack.size > 0) {
			upper = stack.pop();
			lower = stack.pop();
			if (upper <= lower)
				continue;
			int i = quicksortPartition(values, lower, upper);
			if (i - lower > upper - i) {
				stack.add(lower);
				stack.add(i - 2);
			}
			stack.add(i + 2);
			stack.add(upper);
			if (upper - i >= i - lower) {
				stack.add(lower);
				stack.add(i - 2);
			}
		}
	}

	private int quicksortPartition(final float[] values, int lower, int upper) {
		float value = values[lower];
		int up = upper;
		int down = lower;
		float temp;
		while (down < up) {
			while (values[down] <= value && down < up)
				down = down + 2;
			while (values[up] > value)
				up = up - 2;
			if (down < up) {
				temp = values[down];
				values[down] = values[up];
				values[up] = temp;

				temp = values[down + 1];
				values[down + 1] = values[up + 1];
				values[up + 1] = temp;
			}
		}
		values[lower] = values[up];
		values[up] = value;

		temp = values[lower + 1];
		values[lower + 1] = values[up + 1];
		values[up + 1] = temp;
		return up;
	}
}
