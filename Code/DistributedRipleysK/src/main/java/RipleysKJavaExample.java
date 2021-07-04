
import static org.wysrc.localK.analysis.Actuator.executeLocalRipleysK;
import static org.wysrc.spatialK.analysis.Actuator.executeSpatialRipleysK;
import static org.wysrc.spatiotemporalK.analysis.Actuator.executeSpatiotemporalRipleysK;


public class RipleysKJavaExample {

    /**
     * Spark程序入口
     * @param args 入参
     * @throws Exception 直接抛出异常
     */
    public static void main(String[] args) throws Exception {

        //参数分配
        String master = args[0];
        String pointCSVPath = args[1];
        String polygonPath = args[2];
        boolean useEnvelopeBoundary = Boolean.valueOf(args[3]);
        int pointSize = Integer.valueOf(args[4]);
        double maxSpatialDistance = Double.valueOf(args[5]);
        long maxTemporalDistance = Long.valueOf(args[6]);
        String temporalUnit = args[7];
        int spatialStep = Integer.valueOf(args[8]);
        int temporalStep = Integer.valueOf(args[9]);
        String simulationType = args[10];
        int simulationCount = Integer.valueOf(args[11]);
        int numPartitions = Integer.valueOf(args[12]);
        boolean usePartition = Boolean.valueOf(args[13]);
        boolean useIndex = Boolean.valueOf(args[14]);
        boolean useCache = Boolean.valueOf(args[15]);
        boolean useKryo = Boolean.valueOf(args[16]);
        String outputPath = args[17];
        String kfunType = args[18];

        switch (kfunType) {
            case "spatialK":
                executeSpatialRipleysK(master, pointCSVPath, polygonPath, useEnvelopeBoundary, pointSize, maxSpatialDistance, maxTemporalDistance, temporalUnit,
                        spatialStep, temporalStep, simulationType, simulationCount, numPartitions,
                        usePartition, useIndex, useCache, useKryo, outputPath);
                break;
            case "localK":
                executeLocalRipleysK(master, pointCSVPath, polygonPath, useEnvelopeBoundary, pointSize, maxSpatialDistance, maxTemporalDistance, temporalUnit,
                        spatialStep, temporalStep, simulationType, simulationCount, numPartitions,
                        usePartition, useIndex, useCache, useKryo, outputPath);
                break;

            case "spatiotemporalK":
                executeSpatiotemporalRipleysK(master, pointCSVPath, polygonPath, useEnvelopeBoundary, pointSize, maxSpatialDistance, maxTemporalDistance, temporalUnit,
                        spatialStep, temporalStep, simulationType, simulationCount, numPartitions,
                        usePartition, useIndex, useCache, useKryo, outputPath);
                break;
        }
    }



}
