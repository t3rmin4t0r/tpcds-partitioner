package org.notmysock.tpcds;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomOrcOutputFormat extends FileOutputFormat<NullWritable, Text> {

	public static void setColumns(Configuration conf, String columns) {
		conf.set("org.notmysock.tpcds.part.cols", columns);
	}
	
	public static WeakHashMap<Path, RecordWriter> writers = new WeakHashMap<Path, RecordWriter>();
	
	public static void flushWriters(TaskAttemptContext tactxt) throws IOException, InterruptedException {
		for(RecordWriter w: writers.values()) {
			w.close(tactxt);
		}
		writers.clear();
	}
	
	@Override
	public RecordWriter<NullWritable, Text> getRecordWriter(
			TaskAttemptContext tactxt) throws IOException, InterruptedException {
		Configuration conf = tactxt.getConfiguration();
		String columns = conf.get("org.notmysock.tpcds.part.cols");
		ArrayList<String> types = new ArrayList<String>(32);
		for(String t: columns.split(",")) {
			if(t.contains("x")) {
				String fields[] = t.split("x");
				int k = Integer.parseInt(fields[1]);
				for(int i = 0; i < k ; i++)  {
					types.add(fields[0]);
				}
			} else {
				types.add(t);
			}
		}
		OrcFile.WriterOptions opts = OrcFile.writerOptions(conf);
		opts.blockPadding(true).inspector(new RawDataObjectInspector(types.toArray(new String[0])));
		opts.fileSystem(FileSystem.get(conf));
		return new CustomOrcRecordWriter(getDefaultWorkFile(tactxt, ".orc"), opts, types.toArray(new String[0]));
	}
	
	private class CustomOrcRecordWriter extends RecordWriter<NullWritable, Text> {

		private final Path output;
		private Writer writer;
		private final WriterOptions opts;
		private final String[] types;
		
		public CustomOrcRecordWriter(Path output, WriterOptions opts, String[] types) {
			this.output = output;
			this.opts = opts;
			this.types = types;
			CustomOrcOutputFormat.writers.put(output, this);
		}
		
		@Override
		public void close(TaskAttemptContext tactxt) throws IOException,
				InterruptedException {
			if(writer != null) {
				System.err.println("Closing " + output);
				writer.close();
				writer = null;
			}
		}
		
		private Writer getWriter() throws IOException {
			if(writer != null) {
				return writer;
			}
			System.err.println("Opening " + output);
			writer = OrcFile.createWriter(output, opts);
			return writer;
		}

		@Override
		public void write(NullWritable dummy, Text row) throws IOException,
				InterruptedException {
			Writer w = getWriter();
			Object[] r = new Object[types.length];
			String[] v = row.toString().split("\\|");
			for(int i = 0; i < types.length; i++) {
				if(i >= v.length) {
					r[i] = null;
					continue;
				}
				if("int".equals(types[i])) {
					if("".equals(v[i])) {
						r[i] = Integer.valueOf(0);
					} else {
						r[i] = Integer.valueOf(v[i]);
					}
				} else if("float".equals(types[i])) {
					if("".equals(v[i])) {
						r[i] = Double.valueOf(0.0); 
					} else {
						r[i] = Double.valueOf(v[i]);
					}
				} else {
					r[i] = v[i];
				}
			}
			w.addRow(r);
		}
		
	}
}