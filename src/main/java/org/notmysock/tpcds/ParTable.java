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
    
    private enum Tables {
    	store_sales,
    	store_returns,
    	inventory,
    	catalog_sales,
    	catalog_returns,
    	web_sales,
    	web_returns
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

        // defaults to ORC for now
        options.addOption("f", "format", true, "format");
        
        CommandLine line = parser.parse(options, remainingArgs);

        if(!(line.hasOption("input") && line.hasOption("output"))) {
          HelpFormatter f = new HelpFormatter();
          f.printHelp("ParTable", options);
          return 1;
        }
        
        int parallel = 0;
        int key = 0;
        String label = "date";
        String columns = "";
        String sorts = "";

        if(line.hasOption("parallel")) {
          parallel = Integer.parseInt(line.getOptionValue("parallel"));
        }
        
		if (!line.hasOption("table")) {

			if (line.hasOption("key")) {
				key = Integer.parseInt(line.getOptionValue("key"));
			}

			if (line.hasOption("label")) {
				label = line.getOptionValue("label");
			}

			if (line.hasOption("columns")) {
				columns = line.getOptionValue("columns");
			} else {
				System.err.println("Missing columns - should be using something like intx11,floatx12");
				return 1;
			}

			if (line.hasOption("sorted")) {
				sorts = line.getOptionValue("sorted");
			}
		} else {
			Tables t = Tables.valueOf(line.getOptionValue("table"));
			key = 0; // for all tables
			switch(t) {
				case store_sales: {
					label = "ss_sold_date";
					columns = "intx11,floatx12";
					sorts = "2,7"; // item_sk and store_sk
				} break;
				case store_returns: {
					label = "sr_returned_date";
					columns = "intx11,floatx9";
					sorts = "2"; // item_sk
				} break;
				case web_sales: {
					label = "ws_sold_date";
					columns = "intx19,floatx15";
					sorts = "3"; // item_sk
				} break;
				case web_returns: {
					label = "wr_returned_date";
					columns = "intx15,floatx9";
					sorts = "2";
				} break;
				case catalog_sales: {
					label = "cs_sold_date";
					columns = "intx19,floatx15";
					sorts = "15,6";
				} break;
				case catalog_returns: {
					label = "cr_returned_date";
					columns = "intx18,floatx9";
					sorts="2,3";
				} break;
				case inventory: {
					label = "inv_date";
					columns = "intx4";
					sorts = "1,2";
				} break;
				default: {
					System.err.println("Unknown table type " + line.getOptionValue("table"));
					return 1;
				}
			}
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
        conf.set("org.notmysockj.tpcds.part.sort", sorts);
        conf.set("org.notmysock.tpcds.part.label", label);
        conf.set("org.notmysock.tpcds.part.cols", columns);
        conf.setBoolean("mapred.compress.map.output", true);
        
        FileSystem fs = FileSystem.get(getConf());
        Path hiveJar = new Path(tmpPath, "hive-exec.jar");
		fs.copyFromLocalFile(new Path(Utilities
				.findContainingJar(OrcFile.class).toURI()), hiveJar);
		fs.deleteOnExit(hiveJar);
		fs.deleteOnExit(tmpPath);
		
		if(parallel == 0) {
			final long reducerSize = 512*1024*1024;
			ContentSummary cs = fs.getContentSummary(in);
			long size = cs.getSpaceConsumed();
			parallel = (int)(size/reducerSize);
			System.out.println("Estimating reducer count to " + parallel);
		}
		
        DistributedCache.addArchiveToClassPath(hiveJar, conf);

        Job job = new Job(conf, "ParTable_orc " + in);
        job.setJarByClass(getClass());
        job.setNumReduceTasks(parallel);
        job.setInputFormatClass(KeyInputFormat.class);
        job.setReducerClass(MultiFileSink.class);
        job.setOutputKeyClass(Key.class);
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
    
    public static final class Key implements Writable, WritableComparable<Key> {
    	String path = null;
    	int[] fields = new int[0];
    	
		@Override
		public void readFields(DataInput in) throws IOException {
			this.path = in.readUTF();
			final int n = in.readUnsignedByte();
			this.fields = new int[n];
			for(int i = 0; i < n; i++) {
				this.fields[i] = in.readInt();
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {		
			out.writeUTF(path);
			out.writeByte(fields.length);
			for(int x: fields) {
				out.writeInt(x);
			}
		}
		
		/* This prevents bad keys from overloading a single reducer */
		public static int sprayHash = 0;
		
		@Override 
		public int hashCode() {
			if(this.path.contains("__HIVE_DEFAULT_PARTITION__")) {
				// generate at most 61 files
				return this.path.hashCode() + ((sprayHash++) % 61);
			}
			return this.path.hashCode();
		}

		@Override
		public int compareTo(Key k1) {
			int v1 = k1.path.compareTo(this.path);
			if(v1 == 0) {
				for(int i = 0; i < this.fields.length ; i++) {
					if (k1.fields[i] != this.fields[i]) {
						return (this.fields[i] < k1.fields[i]) ? -1 : 1;
					}
				}
			}
			return v1;
		}
     }
    
    public static final class MultiFileSink extends Reducer<Key, Text, Text, Text> {
    	private MultipleOutputs<Text, Text> mos;
    	private String previousPath = null;

		protected void setup(Context context) throws IOException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
		}
		
		@Override
	    public void reduce(Key key, Iterable<Text> values, Context context) 
	        throws IOException, InterruptedException {
			String path = key.path;
			if(previousPath != null && previousPath.equals(path) == false) {
				// new path, close old file
				CustomOrcOutputFormat.flushWriters(context);
			}
			for(Text v: values) {
				mos.write("orc", null, v, path);
			}
			previousPath = path;
		}
    }
    
    public static final class KeyInputFormat extends FileInputFormat<Key, Text> {
    	  @Override
          public RecordReader<Key,Text> createRecordReader(InputSplit input,  TaskAttemptContext context)
              throws IOException, InterruptedException {
        	int key = context.getConfiguration().getInt("org.notmysock.tpcds.part.key", 0);
        	int[] sorts = Utilities.parseSorts(context.getConfiguration().get("org.notmysock.tpcds.part.sort"));
            RecordReader<Key, Text> reader =  new KeyReader(key, sorts);
            reader.initialize(input, context);
            return reader;
          }
          @Override
          protected boolean isSplitable(JobContext context, Path filename) {
            return false;
          }
    }
    
    public static final class KeyReader extends RecordReader<Key,Text> {
		private LineRecordReader lineReader = new LineRecordReader();
		private Key key = new Key();
		private Text value = new Text();
		private int keyPos = 0;
		private int[] sortKeys = new int[0];

		private String dt = "1900-01-02"; // Start date
		private int DATE_BASE = 2415022;
		private String dt_label = "date";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
    	 
		public KeyReader(int keyPos, int[] sortKeys) {
			this.keyPos = keyPos;
			this.sortKeys = sortKeys;
			key.fields = new int[sortKeys.length];
		}

		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException {
			dt_label = context.getConfiguration().get("org.notmysock.tpcds.part.label");
			lineReader.initialize(split, context);
		}

		public Key getCurrentKey() {
			return key;
		}

		public Text getCurrentValue() {
			return value;
		}

		private String getPath(String key) throws IOException {
			if(key.equals("")) {
				return String.format("%s=__HIVE_DEFAULT_PARTITION__/", dt_label);
			}
			Integer off;
			try {
				off = Integer.valueOf(key);
			} catch(NumberFormatException ne) {
				return null;
			}
			try {
				c.setTime(sdf.parse(dt));
			} catch(ParseException pe) {
				throw new IOException(pe);
			}
			c.add(Calendar.DATE, off.intValue() - DATE_BASE);  // number of days to add
			return String.format("%s=%s/", dt_label, sdf.format(c.getTime()));  // dt is now the new date
		}

		public boolean nextKeyValue() throws IOException {
			// get the next line
			if (!lineReader.nextKeyValue()) {
				return false;
			}
			Text lineValue = lineReader.getCurrentValue();
			String[] cols = lineValue.toString().split("\\|");
			if(keyPos < cols.length) {
				key.path = getPath(cols[keyPos]);
			} else {
				key.path = getPath("");
			}
			if(key.path == null) {
				// not well-formed row
				return nextKeyValue();
			}
			for(int i: this.sortKeys) {
				int k = Math.abs(i);
				if(cols.length <= k) {
					key.fields[k] = 0;
				} else if ("".equals(cols[i])) {
					key.fields[k] = 0;
				} else {
					key.fields[k] = Integer.signum(i)
							* Integer.parseInt(cols[k]);
				}
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
