package org.notmysock.tpcds;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;
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

        CommandLineParser parser = new BasicParser();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        options.addOption("i","input", true, "input");
        options.addOption("o","output", true, "output");
        options.addOption("p", "parallel", true, "parallel");
        options.addOption("k", "key", true, "key");
        CommandLine line = parser.parse(options, remainingArgs);

        if(!(line.hasOption("input") && line.hasOption("output"))) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("GenTable", options);
          return 1;
        }
        
        int parallel = 100;
        int key = 0;

        if(line.hasOption("parallel")) {
          parallel = Integer.parseInt(line.getOptionValue("parallel"));
        }
        
        if(line.hasOption("key")) {
         key = Integer.parseInt(line.getOptionValue("key"));
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
        conf.setBoolean("mapred.compress.map.output", true);
        
        Job job = new Job(conf, "ParTable " + in);
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
        boolean success = job.waitForCompletion(true);

        // cleanup
        FileSystem fs = FileSystem.get(getConf());

        return 0;
    }
    
    public static final class MultiFileSink extends Reducer<Text, Text, Text, Text> {
    	private MultipleOutputs<Text, Text> mos;
		private String dt = "1901-01-02";  // Start date
		private int DATE_BASE = 2415022;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();

		protected void setup(Context context) throws IOException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
		private String getPath(String key) throws IOException {
			if(key.equals("")) {
				return "sold_date=__null__/";
			}
			Integer off = Integer.valueOf(key);
			try {
				c.setTime(sdf.parse(dt));
			} catch(ParseException pe) {
				throw new IOException(pe);
			}
			c.add(Calendar.DATE, off.intValue() - DATE_BASE);  // number of days to add
			return String.format("sold_date=%s/",sdf.format(c.getTime()));  // dt is now the new date
		}
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	        throws IOException, InterruptedException {
			String path = getPath(key.toString());
			for(Text v: values) {
				mos.write("text", v, null, path);
			}
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
