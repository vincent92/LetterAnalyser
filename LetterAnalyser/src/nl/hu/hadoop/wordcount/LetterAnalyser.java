package nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class LetterAnalyser {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(LetterAnalyser.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(LetterMapper.class);
		job.setReducerClass(LetterReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

class LetterMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {

		String[] words = value.toString().split("\\s");

		for (String word : words) {

			int wordlength = word.length();
			for (int i = 0; i < wordlength; i++) {

				String firstChar = String.valueOf(word.toLowerCase().charAt(i));
				if (i + 1 < wordlength) {
					String secondChar = String.valueOf(word.toLowerCase().charAt(i + 1));
					context.write(new Text(firstChar), new Text(secondChar));
				}
			}
		}
		context.write(new Text(""), new Text(""));
	}
}

class LetterReducer extends Reducer<Text, Text, Text, Text> {

	private static final String[] ALPHABET = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z" };
	private ArrayList<CharRow> rows = new ArrayList<CharRow>();
	private boolean firstRun = true;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		if (firstRun) {
			firstRun = false;
			String firstLine = "";

			for (String character : ALPHABET) {
				rows.add(new CharRow(character, ALPHABET));
				firstLine += (character + "    ");
			}
			context.write(new Text("  "), new Text(firstLine));
		}
		
		CharRow row = null;
		for (CharRow r : rows) {
			if (r.key.equalsIgnoreCase(key.toString()))
				row = r;
		}

		if (row != null) {
			for (Text value : values) {
				row.updateValue(value.toString());
			}
			context.write(new Text(row.key), new Text(row.toString()));
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {		
		String lastLine = generateTotalNumbersLine();
		
		context.write(new Text(""), new Text(lastLine.replaceAll(".", "_")));
		context.write(new Text("total:"), new Text(lastLine));
	}

	private String generateTotalNumbersLine() {
		String line = "";	
		for (String a : ALPHABET) {
			int total = 0;
			for (CharRow row : rows) {
				total += row.getValue(a).count;
			}
			line += total + calculateWhiteSpace(total);
		}
		return line;
	}
	
	public String calculateWhiteSpace(int number) {
		if (number < 10) {
			return "    ";
		} else if (number < 100) {
			return "   ";
		} else if (number < 1000) {
			return "  ";
		} else {
			return " ";
		}
	}
}

class CharRow {

	public String key;
	private ArrayList<CharValue> values = new ArrayList<CharValue>();

	CharRow(String key, String[] alphabet) {
		this.key = key;
		setValues(alphabet);
	}

	public void updateValue(String key) {
		CharValue value = getValue(key);
		if (value != null) 
			value.count++;
	}
	
	public CharValue getValue(String key) {
		CharValue value = null;
		for (CharValue rowValue : getValues()) {
			if (rowValue.key.equalsIgnoreCase(key)) 
				value = rowValue;
		}
		return value;
	}
	
	@Override
	public String toString() {
		int total = 0;
		String valueLine = "";
		for (CharValue value : getValues()) {
			valueLine += value.count + calculateWhiteSpace(value.count);
			total += value.count;
		}
		valueLine += "|" + total;
		return valueLine;
	}

	public String calculateWhiteSpace(int number) {
		if (number < 10) {
			return "    ";
		} else if (number < 100) {
			return "   ";
		} else if (number < 1000) {
			return "  ";
		} else {
			return " ";
		}
	}

	public ArrayList<CharValue> getValues() {
		return values;
	}

	public void setValues(String[] alphabet) {
		for (String character : alphabet) {
			getValues().add(new CharValue(character, 0));
		}
	}
}

class CharValue {

	public String key;
	public int count;

	CharValue(String key, int count) {
		this.key = key;
		this.count = count;
	}
}
