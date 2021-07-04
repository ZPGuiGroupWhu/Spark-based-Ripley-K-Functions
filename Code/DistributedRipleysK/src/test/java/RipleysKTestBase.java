import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.wysrc.spatialK.serde.RipleysKFunctionKryoRegistrator;

public class RipleysKTestBase {
    protected static SparkConf sparkConf;
    protected static JavaSparkContext sc;

    protected static void initialize(final String appName) {
        sparkConf = new SparkConf().setAppName(appName).setMaster("local[*]");
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", RipleysKFunctionKryoRegistrator.class.getName());

        sc = new JavaSparkContext(sparkConf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }
}