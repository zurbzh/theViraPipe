package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by zurbzh on 2018-10-16.
 */
public class SingleNodeSingleAssemler {


    private static String tablename = "records";


    public static void  main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("SingleNodeSingleAssemler");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Options options = new Options();

        Option in = new Option("in", true, "");
        Option out = new Option("out", true, "");
        Option local = new Option("localdir", true, "");
        Option cases = new Option("cases", true, "");


        options.addOption(in);
        options.addOption(out);
        options.addOption(local);
        options.addOption(cases);

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("spark-submit <spark specific args>", options, true);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            System.exit(1);
        }
        String inputPath = (cmd.hasOption("in") == true) ? cmd.getOptionValue("in") : null;
        String outDir = (cmd.hasOption("out") == true) ? cmd.getOptionValue("out") : null;
        String localdir = cmd.getOptionValue("localdir");
        String patients = cmd.getOptionValue("cases");



        FileSystem fs = FileSystem.get(new Configuration());
        fs.mkdirs(fs, new Path(outDir), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

        Path patinentFile = new Path(patients);
        FSDataInputStream inputStream = fs.open(patinentFile);
        //Classical input stream usage
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        ArrayList<String> lines = new ArrayList<String>();
        while ((line = br.readLine()) != null) {
            String caseId = line.split("\t")[0];
            lines.add(caseId);
        }
        br.close();



        inputStream.close();
        //fs.close();

        String tempName = String.valueOf((new Date()).getTime());

        String hdfsTempname = "hdfs:///Projects/TCGA/Resources/" + tempName;


        FileStatus[] files = fs.listStatus(new Path(inputPath));

        for (FileStatus file : files) {
            String fl = file.getPath().toUri().getRawPath();
            List<String> items = Arrays.asList(fl.split("\\s*/\\s*"));
            String name = items.get(items.size() - 1).split("\\.")[0];
            if (lines.contains(name))
            {

                Path srcPath = new Path(file.getPath().toUri().getRawPath());
                Path dstPath = new Path( hdfsTempname+ "/" + name + ".fq");
                FileUtil.copy(fs, srcPath, fs, dstPath, false, new Configuration());

            }
        }




        fs.mkdirs(fs,new Path(outDir),new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL));


        fs.copyToLocalFile(false, new Path(hdfsTempname), new Path(localdir));


        String pathToLocalFasta = localdir + "/" + tempName;

        String cat = "cat " + pathToLocalFasta + "/*.fq > "+pathToLocalFasta+"/fasta.fa";
        executeBashCommand(cat);
        String rm = "rm "+pathToLocalFasta+"/*.fq";
        executeBashCommand(rm);





        String idba = "idba --pre_correction -r "+pathToLocalFasta+"/fasta.fa -o "+pathToLocalFasta+"/idba";
        executeBashCommand(idba);



        File idbaOutput = new File(pathToLocalFasta+"/idba/scaffold.fa");

        if (idbaOutput.exists()) {

            String getAllcontigs = "cat " + pathToLocalFasta + "/idba/scaffold.fa > " + pathToLocalFasta + "/final_contigs.fa";
            executeBashCommand(getAllcontigs);

        } else {
            String getAllcontigs = "cat " + pathToLocalFasta + "/idba/contig.fa > " + pathToLocalFasta + "/final_contigs.fa";
            executeBashCommand(getAllcontigs);
        }


        String final_local_path = pathToLocalFasta +"/final_contigs.fa";
        fs.copyFromLocalFile(new Path(final_local_path), new Path(outDir));


        sc.stop();

    }





    private static JavaRDD<String> dfToFasta(Dataset<Row> df) {

        return df.toJavaRDD().map(row -> {

            String name = row.getAs("key");

            if (row.getAs("read").equals(1)) {
                name = name + "/1";
            } else {
                name = name + "/2";
            }

            String sequence = row.getAs("sequence").toString();
            //TODO: check values
            String output = ">" + name + "\n" + sequence;

            return output;

        });

    }



    private static void executeBashCommand(String command) {
        try {

            System.out.println("bash command - " + command);
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);
            Process process = pb.start();
            BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String e;
            ArrayList<String> out = new ArrayList<String>();
            while ((e = err.readLine()) != null) {
                System.out.println(e);
                out.add(e);
            }
            process.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}
