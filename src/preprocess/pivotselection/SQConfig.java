package preprocess.pivotselection;

public class SQConfig {
	/** metric space */
	public static final String strMetricSpace = "lof.metricspace.dataspace";
	/** metric */
	public static final String strMetric = "lof.metricspace.metric";
	/** number of K */
	public static final String strK = "lof.query.threshold.K";
	/** number of dimensions */
	public static final String strDimExpression = "lof.vector.dim";
	/** ============================= pivot selection ================ */
	/** number of pivots */
	public static final String strNumOfPivots = "lof.pivot.count";
	/** path for pivots */
	public static final String strPivotInput = "lof.pivot.input";
	public static final String strSelStrategy = "lof.pivot.selection.strategy";
	public static final String strSampleSize = "lof.pivot.sample.size";
	public static final String strDatasetSize = "lof.pivot.dataset.size";

	/** dataset original path */
	public static final String dataset = "lof.dataset.input.dir";
	public static final String datasetNoOutliers = "lof.dataset.input.dir.noOutliers";

	/** file extension names of indexes, maintained for DistributedCache */
	public static final String strIndexExpression1 = ".index";
	// public static final String strIndexExpression2 = ".index2";
	public static final String strPivotExpression = ".pivot";
	public static final String strGroupExpression = ".group";

	/** seperator for items of every record in the index */
	public static final String sepStrForIndex = ",";
	public static final String sepStrForRecord = ",";
	public static final String sepStrForKeyValue = "\t";
	public static final String sepStrForIDDist = "|";
	public static final String sepSplitForIDDist = "\\|";
	public static final String sepSplitForGroupIDs = " ";

	public static final String strNumOfReducers = "lof.reducer.count";

	// DataSplit
	public static final String strIndexOutput = "lof.index.output";
	public static final String strMergeIndexOutput = "lof.merge.index.output";
	public static final String strLofInput = "lof.datasplit.output";

	public static final String strKdistanceOutput = "lof.kdistance.output";
	public static final String strLRDOutput = "lof.lrd.output";
	public static final String strLOFOutput = "lof.final.output";

	// public static final String strKNNJoinOutput = "lof.knn.join.output";
	// public static final String strKNNJoinOutputMerge =
	// "lof.knn.join.output.merge";
	// public static final String strKNNJoinOutputMergeToOne =
	// "lof.knn.join.output.merge.toOne";

	/** For k center problem */
	/** domain values */
	public static final String strDomainMin = "lof.sampling.domain.min";
	public static final String strDomainMax = "lof.sampling.domain.max";
	/** number of partitions */
	public static final String strNumOfPartitions = "lof.sampling.partition.count";
	public static final String kCentertempFile = "lof.sampling.kcenter.oneround.output";
	public static final String kCenterFinalOutput = "lof.sampling.kcenter.final.output";
	public static final String kCovertempFile = "lof.sampling.kcover.oneround.output";
	public static final String kCoverFinalOutput = "lof.sampling.kcover.final.output";

	public static final String strNumOfOutliers = "lof.sampling.kcenter.numOutliers";
	public static final String strDistOfG = "lof.sampling.kcenter.distanceG";
	public static final String strKCenterOutlierOuputPath = "lof.sampling.kcenter.outlier.output";
	public static final String strKCenterFinalCenterOutputPath = "lof.sampling.kcenter.finalPath.output";
	/** number of small cells per dimension */
	public static final String strNumOfSmallCells = "lof.sampling.cells.count";
	public static final String maxRepresentative = "lof.max.representative.count";
	
	public static final String samplingPercentage = "lof.sampling.percentage";
}
