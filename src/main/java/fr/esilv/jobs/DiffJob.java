package fr.esilv.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DiffJob {
    
    private static final String OUTPUT_PATH = "bal.db/diff_output";
    
    public static void run(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: diff <parquetDir1> <parquetDir2>");
            System.exit(1);
        }
        
        String parquetDir1 = args[1];
        String parquetDir2 = args[2];
        
        System.out.println("================= Diff Job ===================");
        System.out.println("Parquet Dir 1: " + parquetDir1);
        System.out.println("Parquet Dir 2: " + parquetDir2);
        
        SparkSession spark = SparkSession.builder()
                .appName("BAL Diff Job")
                .config("spark.sql.warehouse.dir", "bal.db")
                .getOrCreate();
        
        try {
            // Charger les deux datasets
            Dataset<Row> dataset1Raw = spark.read()
                    .parquet(parquetDir1)
                    .dropDuplicates("cle_interop");
            
            Dataset<Row> dataset2Raw = spark.read()
                    .parquet(parquetDir2)
                    .dropDuplicates("cle_interop");
            
            // Vérifier si les datasets ont déjà un hash_value
            boolean hasHash1 = java.util.Arrays.asList(dataset1Raw.columns()).contains("hash_value");
            boolean hasHash2 = java.util.Arrays.asList(dataset2Raw.columns()).contains("hash_value");
            
            Dataset<Row> dataset1;
            Dataset<Row> dataset2;
            
            // Calculer le hash si nécessaire
            if (!hasHash1) {
                dataset1 = addHashColumn(dataset1Raw).cache();
            } else {
                dataset1 = dataset1Raw.cache();
            }
            
            if (!hasHash2) {
                dataset2 = addHashColumn(dataset2Raw).cache();
            } else {
                dataset2 = dataset2Raw.cache();
            }
            
            // INSERTS : dans dataset2 mais pas dans dataset1
            Dataset<Row> insertions = dataset2
                    .join(dataset1, dataset2.col("cle_interop").equalTo(dataset1.col("cle_interop")), "left_anti")
                    .withColumn("operation", lit("INSERT"));
            
            // DELETES : dans dataset1 mais pas dans dataset2
            Dataset<Row> deletions = dataset1
                    .join(dataset2, dataset1.col("cle_interop").equalTo(dataset2.col("cle_interop")), "left_anti")
                    .withColumn("operation", lit("DELETE"));
            
            // UPDATES : même clé mais hash différent
            Dataset<Row> updates = dataset2
                    .join(dataset1.select(
                            col("cle_interop"), 
                            col("hash_value").alias("prev_hash")
                    ), "cle_interop")
                    .filter(col("hash_value").notEqual(col("prev_hash")))
                    .drop("prev_hash")
                    .withColumn("operation", lit("UPDATE"));
            
            // Compter et afficher les stats
            long insertCount = insertions.count();
            long deleteCount = deletions.count();
            long updateCount = updates.count();
            
            System.out.println("Insertions: " + insertCount);
            System.out.println("Deletions: " + deleteCount);
            System.out.println("Updates: " + updateCount);
            
            dataset1.unpersist();
            dataset2.unpersist();
            
            System.out.println("====== Diff Job Completed Successfully =======");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
    
    private static Dataset<Row> addHashColumn(Dataset<Row> dataset) {
        // Trier les colonnes alphabétiquement pour cohérence
        String[] sortedColumns = java.util.Arrays.stream(dataset.columns())
                .sorted()
                .toArray(String[]::new);
        
        org.apache.spark.sql.Column[] cols = java.util.Arrays.stream(sortedColumns)
                .map(org.apache.spark.sql.functions::col)
                .toArray(org.apache.spark.sql.Column[]::new);
        
        return dataset.withColumn("hash_value", sha2(concat_ws("||", cols), 256));
    }
}