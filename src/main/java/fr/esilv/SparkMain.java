package fr.esilv;

import fr.esilv.jobs.DiffJob;
import fr.esilv.jobs.IntegrationJob;
import fr.esilv.jobs.RecomputeJob;
import fr.esilv.jobs.ReportJob;

public class SparkMain {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: fr.esilv.SparkMain <command> [args...]");
            System.exit(1);
        }

        String command = args[0];

        switch (command) {
            case "diff":
                DiffJob.run(args);
                break;
            case "integration":
                IntegrationJob.run(args);
                break;
            case "report":
                ReportJob.run(args);
                break;
            case "recompute":
                RecomputeJob.run(args);
                break;
            default:
                System.err.println("Commande inconnue : " + command);
                System.exit(1);
        }
    }
}