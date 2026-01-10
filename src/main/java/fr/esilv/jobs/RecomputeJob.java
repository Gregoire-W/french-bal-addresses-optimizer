package fr.esilv.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class RecomputeJob {
    
    private static final String BAL_DIFF_PATH = "bal.db/bal_diff";
    
    public static void run(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: recompute <date> <outputDir>");
            System.exit(1);
        }
        
        String targetDate = args[1];
        String outputDir = args[2];
        
        System.out.println("=============== Recompute Job ================");
        System.out.println("Target date: " + targetDate);
        System.out.println("Output directory: " + outputDir);
        
        SparkSession spark = SparkSession.builder()
                .appName("BAL Recompute Job - " + targetDate)
                .config("spark.sql.warehouse.dir", "bal.db")
                .getOrCreate();
        
        spark.sparkContext().setLogLevel("WARN");
        
        try {
            // Charger tous les diffs jusqu'à la date cible
            Dataset<Row> allDiffs = spark.read()
                    .parquet(BAL_DIFF_PATH)
                    .filter(col("day").leq(targetDate))
                    .cache();
            
            long totalDiffs = allDiffs.count();
            System.out.println("Total diffs to process: " + totalDiffs);
            
            if (totalDiffs == 0) {
                System.out.println("No data found for date <= " + targetDate);
                return;
            }
            
            // Compter les opérations par type
            Dataset<Row> opCounts = allDiffs
                    .groupBy("operation")
                    .count()
                    .orderBy("operation");
            
            System.out.println("\nOperations breakdown:");
            opCounts.show(false);
            
            // Pour chaque cle_interop, garder la dernière opération (par date)
            Dataset<Row> latestOps = allDiffs
                    .withColumn("row_num", row_number().over(
                            org.apache.spark.sql.expressions.Window
                                    .partitionBy("cle_interop")
                                    .orderBy(col("day").desc())
                    ))
                    .filter(col("row_num").equalTo(1))
                    .drop("row_num");
            
            // Ne garder que les INSERT et UPDATE (exclure les DELETE)
            Dataset<Row> finalState = latestOps
                    .filter(col("operation").notEqual("DELETE"))
                    .drop("operation", "day", "hash_value");
            
            long finalCount = finalState.count();
            System.out.println("\nFinal address count at " + targetDate + ": " + finalCount);
            
            // Sauvegarder le dump
            finalState.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(outputDir);
            
            System.out.println("Dump saved to: " + outputDir);
            
            allDiffs.unpersist();
            
            System.out.println("==== Recompute Job Completed Successfully ====");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}