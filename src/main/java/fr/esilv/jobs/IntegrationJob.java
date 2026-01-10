package fr.esilv.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class IntegrationJob {
    
    private static final String BAL_LATEST_PATH = "bal.db/bal_latest";
    private static final String BAL_DIFF_PATH = "bal.db/bal_diff";
    
    public static void run(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: integration <date> <csvFile>");
            System.exit(1);
        }
        
        String date = args[1];
        String csvFile = args[2];
        
        System.out.println("=============== Integration Job ==============");
        System.out.println("Date: " + date);
        System.out.println("CSV File: " + csvFile);
        
        SparkSession spark = SparkSession.builder()
                .appName("BAL Integration Job - " + date)
                .config("spark.sql.warehouse.dir", "bal.db")
                .getOrCreate();
        
        try {
            // Lire et préparer les données courantes
            Dataset<Row> currentDataRaw = spark.read()
                    .option("header", "true")
                    .option("delimiter", ";")
                    .option("encoding", "UTF-8")
                    .csv(csvFile)
                    .dropDuplicates("cle_interop");
            
            // Calculer le hash (colonnes triées pour cohérence)
            String[] sortedColumns = java.util.Arrays.stream(currentDataRaw.columns())
                    .sorted()
                    .toArray(String[]::new);
            
            org.apache.spark.sql.Column[] cols = java.util.Arrays.stream(sortedColumns)
                    .map(org.apache.spark.sql.functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new);
            
            Dataset<Row> currentData = currentDataRaw
                    .withColumn("hash_value", sha2(concat_ws("||", cols), 256))
                    .cache();
            
            // Vérifier si première exécution
            File latestDir = new File(BAL_LATEST_PATH);
            boolean isFirstRun = !latestDir.exists() || latestDir.listFiles() == null || latestDir.listFiles().length == 0;
            
            if (isFirstRun) {
                // Première exécution : tout est INSERT
                long insertCount = currentData.count();
                
                currentData
                        .withColumn("operation", lit("INSERT"))
                        .withColumn("day", lit(date))
                        .write()
                        .mode(SaveMode.Append)
                        .partitionBy("day")
                        .parquet(BAL_DIFF_PATH);
                
                System.out.println("Insertions: " + insertCount);
                System.out.println("Deletions: 0");
                System.out.println("Updates: 0");
                
            } else {
                // Charger les données précédentes
                Dataset<Row> previousData = spark.read()
                        .parquet(BAL_LATEST_PATH)
                        .cache();
                
                // INSERTS
                Dataset<Row> insertions = currentData
                        .join(previousData, currentData.col("cle_interop").equalTo(previousData.col("cle_interop")), "left_anti")
                        .withColumn("operation", lit("INSERT"))
                        .withColumn("day", lit(date));
                long insertCount = insertions.count();
                System.out.println("Insertions: " + insertCount);

                String[] columnOrder = insertions.columns();
                
                // DELETES
                Dataset<Row> deletions = previousData
                        .join(currentData, previousData.col("cle_interop").equalTo(currentData.col("cle_interop")), "left_anti")
                        .withColumn("operation", lit("DELETE"))
                        .withColumn("day", lit(date));
                long deleteCount = deletions.count();
                System.out.println("Deletions: " + deleteCount);
                
                // UPDATES (hash différent)
                Dataset<Row> updates = currentData
                        .join(previousData.select(
                                col("cle_interop"), 
                                col("hash_value").alias("prev_hash")
                        ), "cle_interop")
                        .filter(col("hash_value").notEqual(col("prev_hash")))
                        .drop("prev_hash")
                        .withColumn("operation", lit("UPDATE"))
                        .withColumn("day", lit(date));

                long updateCount = updates.count();
                System.out.println("Updates: " + updateCount);
                
                // Sauvegarder les différences
                org.apache.spark.sql.Column[] cols_union = java.util.Arrays.stream(columnOrder)
                .map(org.apache.spark.sql.functions::col)
                .toArray(org.apache.spark.sql.Column[]::new);

                updates = updates.select(cols_union);
                deletions = deletions.select(cols_union);
                Dataset<Row> allDiffs = insertions.union(updates).union(deletions);
                long totalDiffs = allDiffs.count();
                
                if (totalDiffs > 0) {
                    allDiffs.write()
                            .mode(SaveMode.Append)
                            .partitionBy("day")
                            .parquet(BAL_DIFF_PATH);
                }
                
                previousData.unpersist();
            }
            
            // Mettre à jour bal_latest
            currentData.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(BAL_LATEST_PATH);
            
            currentData.unpersist();
            
            System.out.println("=== Integration Job Completed Successfully ===");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}