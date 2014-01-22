package org.notmysock.tpcds;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.reduce.*;
import org.apache.commons.cli.*;
import org.apache.commons.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.net.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.text.ParseException;


public class ParTable extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new ParTable(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        String uuid = UUID.randomUUID().toString();
        Path tmpPath = new Path("/tmp/"+uuid+"/");
        
        CommandLineParser parser = new BasicParser();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("i","input", true, "input");
        options.addOption("o","output", true, "output");
        options.addOption("p", "parallel", true, "parallel");
        options.addOption("k", "key", true, "key");
        options.addOption("l", "label", true, "label");
        options.addOption("c", "columns", true, "columns");
        options.addOption("s", "sorted", true, "sorted");
        
        options.addOption("t", "table", true, "table");
        
        CommandLine line = parser.parse(options, remainingArgs);

        if(!(line.hasOption("input") && line.hasOption("output"))) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("ParTable", options);
          return 1;
        }
        
        int parallel = 100;
        int key = 0;
        String label = "date";
        String columns = "";

        if(line.hasOption("parallel")) {
          parallel = Integer.parseInt(line.getOptionValue("parallel"));
        }
        
        if(line.hasOption("key")) {
            key = Integer.parseInt(line.getOptionValue("key"));
        }
        
        if(line.hasOption("label")) {
        	label = line.getOptionValue("label");
        }
        
        if(line.hasOption("columns")) {
        	columns = line.getOptionValue("columns");
        } else {
        	System.err.println("Missing columns - should be using something like intx11,floatx12");
        }
        
        if(parallel == 1) {
          System.err.println("The MR task does not work for scale=1 or parallel=1");
          return 1;
        }

        Path in = new Path(line.getOptionValue("input"));
        Path out = new Path(line.getOptionValue("output"));
        
        Configuration conf = getConf();
        
        conf.setInt("mapred.task.timeout",0);
        conf.setInt("mapreduce.task.timeout",0);
        conf.setInt("org.notmysock.tpcds.part.key", key);
        conf.set("org.notmysock.tpcds.part.label", label);
        conf.set("org.notmysock.tpcds.part.cols", columns);
        conf.setBoolean("mapred.compress.map.output", true);
        
        FileSystem fs = FileSystem.get(getConf());
        Path hiveJar = new Path(tmpPath, "hive-exec.jar");
		fs.copyFromLocalFile(new Path(Utilities
				.findContainingJar(OrcFile.class).toURI()), hiveJar);
		fs.deleteOnExit(hiveJar);
		
        DistributedCache.addArchiveToClassPath(hiveJar, conf);

        Job job = new Job(conf, "ParTable_orc " + in);
        job.setJarByClass(getClass());
        job.setNumReduceTasks(parallel);
        job.setInputFormatClass(KeyInputFormat.class);
        job.setReducerClass(MultiFileSink.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);

        // use multiple output to only write the named files
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "text", 
          TextOutputFormat.class, LongWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "orc", 
                CustomOrcOutputFormat.class, NullWritable.class, Text.class);
        boolean success = job.waitForCompletion(true);

        return 0;
    }
    
    public static final class MultiFileSink extends Reducer<Text, Text, Text, Text> {
    	private MultipleOutputs<Text, Text> mos;
		private String dt = "1900-01-02";  // Start date
		private int DATE_BASE = 2415022;
		private String dt_label = "sold_date";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();

		protected void setup(Context context) throws IOException {
			mos = new MultipleOutputs<Text, Text>(context);
			dt_label = context.getConfiguration().get("org.notmysock.tpcds.part.label");
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
		
		private String getPath(String key) throws IOException {
			if(key.equals("")) {
				return String.format("%s=__HIVE_DEFAULT_PARTITION__/", dt_label);
			}
			Integer off = Integer.valueOf(key);
			try {
				c.setTime(sdf.parse(dt));
			} catch(ParseException pe) {
				throw new IOException(pe);
			}
			c.add(Calendar.DATE, off.intValue() - DATE_BASE);  // number of days to add
			return String.format("%s=%s/", dt_label, sdf.format(c.getTime()));  // dt is now the new date
		}
		
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	        throws IOException, InterruptedException {
			String path = getPath(key.toString());
			for(Text v: values) {
				mos.write("orc", null, v, path);
			}
			CustomOrcOutputFormat.flushWriters(context);
		}
    }
    
    public static final class KeyInputFormat extends FileInputFormat<Text, Text> {
    	  @Override
          public RecordReader<Text,Text> createRecordReader(InputSplit input,  TaskAttemptContext context)
              throws IOException, InterruptedException {
        	int key = context.getConfiguration().getInt("org.notmysock.tpcds.part.key", 0);
            RecordReader<Text, Text> reader =  new KeyReader(key);
            reader.initialize(input, context);
            return reader;
          }
          @Override
          protected boolean isSplitable(JobContext context, Path filename) {
            return false;
          }
    }
    
    public static final class KeyReader extends RecordReader<Text,Text> {
    	 private LineRecordReader lineReader = new LineRecordReader();
    	 private Text key = new Text();
    	 private Text value = new Text();
    	 private int keyPos = 0;
    	 
    	 public KeyReader(int key) {
    		 this.keyPos = key;
    	 }
    	 
    	 public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    		 lineReader.initialize(split, context);
    	 }
    	 
		public Text getCurrentKey() {
			return key;
		}

		public Text getCurrentValue() {
			return value;
		}

		public boolean nextKeyValue() throws IOException {
			// get the next line
			if (!lineReader.nextKeyValue()) {
				return false;
			}
			Text lineValue = lineReader.getCurrentValue();
			String[] cols = lineValue.toString().split("\\|");
			if(keyPos < cols.length) {
				key.set(cols[keyPos]);
			} else {
				key.set("");
			}
			value.set(lineValue);
			return true;
		}

		public void close() throws IOException {
			lineReader.close();
		}

		public float getProgress() throws IOException {
			return lineReader.getProgress();
		}
    }
}
