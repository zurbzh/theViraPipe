package org.ngseq.metagenomics;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import trimmomatic.*;


public class QualityCheck {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("QualityCheck");

    JavaSparkContext sc = new JavaSparkContext(conf);

    Options options = new Options();

    Option inOpt = new Option( "in", true, " To read data from HDFS subdirectories" );
    Option outOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option phredOpt = new Option("phred", true, "phred value for quality check: default 33");
    Option leadOpt = new Option("leading", true, "value for leading trimming: default 3");
    Option trailOpt = new Option("trailing", true, "value for trailing trimming: default 3");
    Option windowlenOpt = new Option("slidingWindow", true, "length of sliding window: default 4");
    Option minLengthOpt = new Option("minlen", true, "drop the read if it is shorter than this value : default 18");
    Option adaptorsOpt = new Option("adaptors", true, "file in hdfs containing adaptor sequences");



    options.addOption( inOpt );
    options.addOption( outOpt );

    options.addOption( phredOpt );
    options.addOption( leadOpt );
    options.addOption( trailOpt );
    options.addOption( windowlenOpt );

    options.addOption( minLengthOpt );

    options.addOption( adaptorsOpt );



    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;

    try {
      // parse the command line arguments
      cmd = parser.parse( options, args );

    }
    catch( ParseException exp ) {
      // oops, something went wrong
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
    }

    String in = (cmd.hasOption("in")==true)? cmd.getOptionValue("in"):null;
    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    int phred = (cmd.hasOption("phred")==true)? Integer.valueOf(cmd.getOptionValue("phred")):33;
    int leading = (cmd.hasOption("leading")==true)? Integer.valueOf(cmd.getOptionValue("leading")):3;
    int trailing = (cmd.hasOption("trailing")==true)? Integer.valueOf(cmd.getOptionValue("trailing")):3;
    int slidingWindow = (cmd.hasOption("slidingWindow")==true)? Integer.valueOf(cmd.getOptionValue("slidingWindow")):4;
    int minlen = (cmd.hasOption("minlen")==true)? Integer.valueOf(cmd.getOptionValue("minlen")):18;
    String adatptors = (cmd.hasOption("adaptors")==true)? cmd.getOptionValue("adaptors"):null;






    // int minc = (cmd.hasOption("minc")==true)? Integer.parseInt(cmd.getOptionValue("minc")):0;


    FileSystem fs = FileSystem.get(new Configuration());

    FileStatus[] dirs = fs.listStatus(new Path(in));



    for (FileStatus dir : dirs) {
      String path = dir.getPath().toUri().getRawPath();


      JavaPairRDD<Text, SequencedFragment> fqRDD = sc.newAPIHadoopFile(path, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

      JavaPairRDD<Text, SequencedFragment> qualityReadsRDD = fqRDD.mapPartitionsToPair(part -> {
        List<Tuple2<Text, SequencedFragment>> QR = new ArrayList<>();
        Logger logger = new Logger(true, false,false);
        IlluminaClippingTrimmer ict1 = IlluminaClippingTrimmer.makeIlluminaClippingTrimmer(logger, adatptors,2,30,10);
        FastqRecord trailedTrimmed1;
        FastqRecord trailedTrimmed2;
        FastqRecord windowTrimmed1;
        FastqRecord windowTrimmed2;
        FastqRecord leadTrimmed1;
        FastqRecord leadTrimmed2;
        FastqRecord [] fastqR = new FastqRecord[2];
        FastqRecord [] fastqRClipped;






        while (part.hasNext()) {
          Tuple2<Text, SequencedFragment> read1 = part.next();
          String name = read1._1.toString().split("/")[0];
          int key = Integer.parseInt(read1._1.toString().split("/")[1]);
          String seq = read1._2.getSequence().toString();

          String quality = read1._2.getQuality().toString();
          fastqR[0] =  new FastqRecord(name, seq, quality, phred);




          if (part.hasNext()){
            Tuple2<Text, SequencedFragment> read2 = part.next();
            String name2 = read2._1.toString().split("/")[0];
            int key2 = Integer.parseInt(read2._1.toString().split("/")[1]);
            String seq2 = read2._2.getSequence().toString();
            String quality2 = read2._2.getQuality().toString();

            if (name.equals(name2) && key2 == 2 ){

                fastqR[1] =  new FastqRecord(name2, seq2, quality2, phred);
                // trim Illumina adaptors
                fastqRClipped = ict1.processRecords(fastqR);

                if (fastqRClipped[0] !=null && fastqRClipped[1] !=null) {

                    leadTrimmed1= new LeadingTrimmer(leading).processRecord(fastqRClipped[0]);
                    leadTrimmed2= new LeadingTrimmer(leading).processRecord(fastqRClipped[1]);

                } else {
                    continue;
                }
                if (leadTrimmed1 != null && leadTrimmed2 != null) {

                    trailedTrimmed1  = new TrailingTrimmer(trailing).processRecord(leadTrimmed1);
                    trailedTrimmed2  = new TrailingTrimmer(trailing).processRecord(leadTrimmed2);

                } else {
                    continue;
                }
                if (trailedTrimmed1 != null && trailedTrimmed2 != null){

                    windowTrimmed1 = new SlidingWindowTrimmer(slidingWindow,15).processRecord(trailedTrimmed1);
                    windowTrimmed2 = new SlidingWindowTrimmer(slidingWindow,15).processRecord(trailedTrimmed2);


                } else {
                    continue;
                }


                if(windowTrimmed1 !=null && windowTrimmed1.getSequence().length()>=minlen && windowTrimmed2 !=null && windowTrimmed2.getSequence().length()>=minlen) {

                    Text t1 = new Text(windowTrimmed1.getName());
                    SequencedFragment sf1 = new SequencedFragment();
                    sf1.setSequence(new Text(windowTrimmed1.getSequence()));
                    sf1.setQuality(new Text(windowTrimmed1.getQuality()));

                    QR.add(new Tuple2<>(t1, sf1));

                    Text t2 = new Text(windowTrimmed2.getName());
                    SequencedFragment sf2 = new SequencedFragment();
                    sf2.setSequence(new Text(windowTrimmed2.getSequence()));
                    sf2.setQuality(new Text(windowTrimmed2.getQuality()));

                    QR.add(new Tuple2<>(t2, sf2));

                }

            } else {
                System.out.println("Single read: " + name);

            }

          }






        }

        return QR.iterator();
      });




      String dr = dir.getPath().toUri().getRawPath();
      List<String> items = Arrays.asList(dr.split("\\s*/\\s*"));

      String name = items.get(items.size() - 1);

      qualityReadsRDD.coalesce(1).saveAsNewAPIHadoopFile(outDir + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
    }

    sc.stop();

  }






}