package fr.esilv.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

import static org.apache.spark.sql.functions.*;

public class ReportJob {
    
    private static final String BAL_LATEST_PATH = "bal.db/bal_latest";
    
    public static void run(String[] args) {
        System.out.println("=============== BAL Report Job ===============");
        
        SparkSession spark = SparkSession.builder()
                .appName("BAL Report")
                .config("spark.sql.warehouse.dir", "bal.db")
                .getOrCreate();
        
        try {
            // Vérifier si bal_latest existe
            File latestDir = new File(BAL_LATEST_PATH);
            if (!latestDir.exists() || latestDir.listFiles() == null || latestDir.listFiles().length == 0) {
                System.out.println("No data found. Run integration job first.");
                return;
            }
            
            // Charger les données
            Dataset<Row> data = spark.read().parquet(BAL_LATEST_PATH);
            
            // Statistiques globales
            long totalAddresses = data.count();
            long totalCommunes = data.select("commune_insee").distinct().count();
            
            System.out.println("\n=== Global Statistics ===");
            System.out.println("Total addresses: " + totalAddresses);
            System.out.println("Total communes: " + totalCommunes);
            
            // Statistiques par département
            Dataset<Row> deptStats = data
                    .withColumn("departement", substring(col("commune_insee"), 1, 2))
                    .groupBy("departement")
                    .agg(
                            count("*").alias("addresses"),
                            countDistinct("commune_insee").alias("communes")
                    )
                    .orderBy(col("departement"));
            
            long totalDepts = deptStats.count();
            
            System.out.println("\n=== Top 10 Departments ===");
            deptStats.orderBy(col("addresses").desc())
                    .select(
                            col("departement"),
                            format_number(col("addresses"), 0).alias("addresses"),
                            col("communes")
                    )
                    .show(10, false);
            
            System.out.println("\n=== Department Summary ===");
            deptStats.agg(
                    lit(totalDepts).alias("departments"),
                    format_number(avg("addresses"), 0).alias("avg_addresses"),
                    format_number(min("addresses"), 0).alias("min_addresses"),
                    format_number(max("addresses"), 0).alias("max_addresses")
            ).show(false);
            
            System.out.println("======= Report Completed Successfully ========");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}