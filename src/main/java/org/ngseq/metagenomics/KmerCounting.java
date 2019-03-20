package org.ngseq.metagenomics;

import org.apache.commons.cli.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Created by zurbzh on 2018-10-10.
 */



public class KmerCounting {


    private static final Logger LOG = Logger.getLogger(KmerCounting.class.getName());


    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("KmerCounting");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);


        Options options = new Options();

        Option pathOpt = new Option("in", true, "Path to file in hdfs."); // csv file: id, sequence, label

        Option outOpt = new Option("out", true, "HDFS path for output files.");

        options.addOption( pathOpt );
        options.addOption( outOpt );
        options.addOption(new Option( "k", true, "kmer size" ));


        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "spark-submit <spark specific args>", options, true );

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse( options, args );
        }
        catch( ParseException exp ) {
            System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
        }

        String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
        String in = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
        int k = (cmd.hasOption("k")==true)? Integer.parseInt(cmd.getOptionValue("k")):16;




        ArrayList<String> kmers =  generateKmers(k);

        Broadcast<ArrayList<String>> broadcastedkmers = sc.broadcast(kmers);

        JavaRDD<String> csvfile = sc.textFile(in);

        JavaRDD<String> partitioned = csvfile.repartition(100);

        JavaPairRDD<String, ArrayList<String>> getCountedKmers = partitioned.mapPartitionsToPair(records -> {

            ArrayList<Tuple2<String, ArrayList<String>>> id_kvalues = new ArrayList<Tuple2<String, ArrayList<String>>>();
            ArrayList<String> kmer_set = new ArrayList<String>();
            while (records.hasNext()) {
                String[] lines = records.next().split(",");


                String id = lines[0].toString();
                String seq = lines[1].toString();
                String species = lines[2].toString();

                //HashSet<String> umer_in_seq = new HashSet<String>();
                for (int i = 0; i < seq.length() - k - 1; i++) {
                    String kmer = seq.substring(i, i + k);
                    kmer_set.add(kmer);
                }

                Map<String, Long> result =
                        kmer_set.stream().collect(
                                Collectors.groupingBy(
                                        Function.identity(), Collectors.counting()
                                )
                        );


                // sort found kmers according to kmers generated by the method: generateKmers()
                ArrayList<String> codons = new ArrayList<String>();

                for (String br : broadcastedkmers.getValue()) {
                    if (result.containsKey(br)) {
                        codons.add(result.get(br).toString());

                    } else {
                        int value = 0;
                        codons.add(Integer.toString(value));
                    }

                }
                codons.add(species);
                id_kvalues.add(new Tuple2<String, ArrayList<String>>(id,codons));


                kmer_set.clear();
            }



            return id_kvalues.iterator();
        });


        JavaRDD<String> id_kmers_species = getCountedKmers.mapPartitions(records -> {

            ArrayList<String> vectors = new ArrayList<String>();
            while (records.hasNext()) {
                Tuple2<String, ArrayList<String>> seq = records.next();
                String id = seq._1();
                ArrayList<String> arr = new ArrayList<String>(seq._2);
                String line = "";
                for (String l : arr) {
                    line += "\t" +  l.toString();
                }
                String vector = id + line;
                vectors.add(vector);
            }
            return vectors.iterator();
        });



        id_kmers_species.saveAsTextFile(outDir);


         sc.stop();
    }





    // generate all possible k size kmers
    public static ArrayList<String> generateKmers(int k)
    {
        int switcher;
        int[] indices = new int[k];
        ArrayList<String> nucleotides = new ArrayList<String>(){{add("A");add("C");add("G");add("T");}};
        ArrayList<String> kmers = new ArrayList<String>();

        do
        {
            String kmer = "";
            for(int index : indices) {

                kmer += nucleotides.get(index);
            }
            kmers.add(kmer);
            switcher = 1;
            for(int i = indices.length - 1; i >= 0; i--)
            {

                if(switcher == 0)
                    break;
                indices[i] += switcher;
                switcher = 0;

                if(indices[i] == nucleotides.size())
                {
                    switcher = 1;
                    indices[i] = 0;
                }
            }
        }
        while(switcher != 1); // Call this method iteratively until a carry is left over
        return kmers;
    }
}
