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

/**THIS IS IN MEMORY IMPLEMENTATION OF PARALLEL BWA AND READ FILTERING, NO READ SPLIT FILES ARE WRITTEN
 * Usage
 spark-submit --master local[${NUM_EXECUTORS}] --executor-memory 20g --class org.ngseq.metagenomics.NormalizeRDD metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -out ${OUTPUT_PATH}/${PROJECT_NAME}_normalized -k ${NORMALIZATION_KMER_LEN} -C ${NORMALIZATION_CUTOFF}

 spark-submit --master yarn --deploy-mode ${DEPLOY_MODE} --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.cachedExecutorIdleTimeout=100 --conf spark.shuffle.service.enabled=true --conf spark.scheduler.mode=${SCHEDULER_MODE} --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --executor-memory 20g --conf spark.yarn.executor.memoryOverhead=10000  --class org.ngseq.metagenomics.NormalizeRDD metagenomics-0.9-jar-with-dependencies.jar -in ${OUTPUT_PATH}/${PROJECT_NAME}_aligned -out ${OUTPUT_PATH}/${PROJECT_NAME}_normalized -k ${NORMALIZATION_KMER_LEN} -C ${NORMALIZATION_CUTOFF}

 **/


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
        FastqRecord trailedTrimmed;
        FastqRecord windowTrimemed;
        FastqRecord leadTrimmed;
        FastqRecord [] fastqR = new FastqRecord[1];
        FastqRecord [] fastqRClipped;


        while (part.hasNext()) {
          Tuple2<Text, SequencedFragment> fastq = part.next();
          String name = fastq._1.toString();
          String seq = fastq._2.getSequence().toString();
          String quality = fastq._2.getQuality().toString();


          fastqR[0] =  new FastqRecord(name, seq, quality, phred);
          fastqRClipped = ict1.processRecords(fastqR);

          if (fastqRClipped[0] != null) {
            leadTrimmed= new LeadingTrimmer(leading).processRecord(fastqRClipped[0]);
          } else {
            continue;
          }
          if (leadTrimmed != null) {
            trailedTrimmed  = new TrailingTrimmer(trailing).processRecord(leadTrimmed);
          } else {
            continue;
          }
          if (trailedTrimmed != null){
            windowTrimemed = new SlidingWindowTrimmer(slidingWindow,15).processRecord(trailedTrimmed);
          } else {
            continue;
          }


          if(windowTrimemed !=null && windowTrimemed.getSequence().length()>=minlen) {
            Text t = new Text(windowTrimemed.getName());
            SequencedFragment sf = new SequencedFragment();
            sf.setSequence(new Text(windowTrimemed.getSequence()));
            sf.setQuality(new Text(windowTrimemed.getQuality()));

            QR.add(new Tuple2<>(t, sf));
          }

        }

        return QR.iterator();
      });




      String dr = dir.getPath().toUri().getRawPath();
      List<String> items = Arrays.asList(dr.split("\\s*/\\s*"));

      String name = items.get(items.size() - 1);

      qualityReadsRDD.distinct().saveAsNewAPIHadoopFile(outDir + "/" + name, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
    }

    sc.stop();

  }






}