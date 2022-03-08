package eu.spierling;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class App {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("My Dummy App");
        conf.setMaster("spark://127.0.0.1:7077");
        conf.setJars(new String[]{"target/_spark-1.0-SNAPSHOT.jar"}); // dirty
        JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        List<Integer> collection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // parallelize the collection to two partitions

        JavaRDD<Integer> rdd = sparkContext.parallelize(collection);

        System.out.println("Number of partitions : "+rdd.getNumPartitions());

        rdd.foreach(new VoidFunction<Integer>(){
            public void call(Integer number) {
                System.out.println(number);
            }});
    }
}
