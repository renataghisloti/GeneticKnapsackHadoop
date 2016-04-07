package org.myorg;
        
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Genetic{
        
    /** Map Phase **/  
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	
    	private Text word = new Text();
        
    	public void map(LongWritable key, Text value, Context context) 
    			throws IOException, InterruptedException {

		String line, temp;
        String [ ] weights;
        String [ ] values;
        String [] randomResult;
        String [] result2;
        StringTokenizer tokenizer;
        int i, size, capacity, j;
        int [] totalValue;
        int []usedNumbers;
        int []selectedInputs;
        Text finalresult;

        // Read input
        line = value.toString();
        tokenizer = new StringTokenizer(line);
        
        if(tokenizer.hasMoreTokens() == false) return;
        temp = tokenizer.nextToken();
 
        capacity = Integer.parseInt(temp.toString().trim());
        temp = tokenizer.nextToken();
        size = Integer.parseInt(temp.toString().trim());
        weights = new String [size];
        values = new String [size];

        i = 0;
        while (tokenizer.hasMoreTokens()) {
        	temp = tokenizer.nextToken();
        	if( (i < size)) 
        		weights[i] = temp;
        	else if( i>= size)
        		values[i - size] = temp;
        	i++;
        }

	    // Start Generating random solutions
	    randomResult = new String [5];

	    // Profit in the solution so far
	    totalValue = new int [5];

	    // Number used in the sultion, so that it doesnt repeat items
        usedNumbers = new int [size];

	    // Each map computes 5 random solutions
	    for(j = 0; j <5; j++) {
	    	if(weights[0] != null) {
	    		usedNumbers = generateRandomInput(capacity, size, weights);
	    		randomResult[j] = calculateItems(usedNumbers, size);
	    		totalValue[j] = calculateProfit(usedNumbers, values,size);
	    	}
	    }

  	    // SELECTION Phase
	    finalresult = new Text();
	    selectedInputs = new int[3];
	    selectedInputs = max(totalValue);

	    result2 = new String [5];

	    // Crossover and mutation Phase
	    for(j = 0; j <5; j++) {
			if(weights[0] != null) {
			    usedNumbers = crossOver(randomResult, selectedInputs, capacity, 3, size, weights);
			    result2[j] = calculateItems(usedNumbers, size);
			    totalValue[j] = calculateProfit(usedNumbers, values,size);
	
			    word.set(String.valueOf(totalValue[j]));
			    finalresult.set(result2[j]);
			    context.write(word, finalresult);
			}
	    }

    }


    // Do crossover between selected inputs
    public static int [] crossOver(String [] result, int []selectedInputs, int capacity, 
    								int selectionSize, int size, String [] weights) {
        int []usedNumbers;
        int i;
        int totalWeight;

        usedNumbers = new int [size];
        
        for(i=0; i<size; i++)
        	usedNumbers[i] = 0;

        i = 0;

        // Weight in the solution so far
        totalWeight = 0;

        while(totalWeight <= capacity) {
        	// Get a random number from the selected results
	      	int randNumber = 0 + (int)(Math.random() * ((selectionSize - 0)));

	      	// Convert result from string to int
	      	String[] strArray = result[selectedInputs[randNumber]].split(" ");
	      	int[] resultIntArray = new int[strArray.length];
	      	for(int j = 0; j < strArray.length; j++) {
	      		resultIntArray[j] = Integer.parseInt(strArray[j]);
	      	}

	      	int index = 0 + (int)(Math.random() * ((strArray.length - 0)));
	        int test = totalWeight + Integer.parseInt(weights[resultIntArray[index]]);

	        if(usedNumbers[resultIntArray[index]] == 0 && (test < capacity)) { 
	        	usedNumbers[resultIntArray[index]] = 1;
	            totalWeight = totalWeight + Integer.parseInt(weights[resultIntArray[index]]);
	            i++;
	        }
	        // check if it is possible to improve
	        if(totalWeight + getMinWeight(weights, usedNumbers, capacity) > capacity ) break;

	        // MUTATION
	        index = 0 + (int)(Math.random() * ((size - 0)));
	        test = totalWeight + Integer.parseInt(weights[index]);

	        if((usedNumbers[index] == 0) && (test <= capacity)) { 
	        	usedNumbers[index] = 1;
	            totalWeight = totalWeight + Integer.parseInt(weights[index]);
	        }
	        if(totalWeight + getMinWeight(weights, usedNumbers, capacity) > capacity ) break;
	    }
        return usedNumbers;
    }

    public static int getMinWeight(String [] weights, int [] usedNumbers, int capacity) {
    	int min = capacity;

    	for(int j = 0; j < weights.length; j++) {
    		if((min > Integer.parseInt(weights[j])) && usedNumbers[j] == 0) {
    			min = Integer.parseInt(weights[j]);
    		}
    	}
        return min;
    }

    public static int [] generateRandomInput(int capacity, int size, String [] weights) {
	
    	// Number already used in the solution, so that it doesnt repeat items
        int []usedNumbers = new int [size];
        int i;
        int totalWeight;

        for(i=0; i<size; i++)
        	usedNumbers[i] = 0;

        // Weight in the solution so far
        totalWeight = 0;

        while(totalWeight <= capacity) {
        	int randNumber = 0 + (int)(Math.random() * ((size - 0)));

        	int test = totalWeight + Integer.parseInt(weights[randNumber]);

        	if((usedNumbers[randNumber] == 0) && (test <= capacity)) { 
        		usedNumbers[randNumber] = 1;
        		totalWeight = totalWeight + Integer.parseInt(weights[randNumber]);
        	}
        	if((totalWeight + getMinWeight(weights, usedNumbers, capacity)) > capacity ) break;
        }
        
        return usedNumbers;
    }

    public static int calculateProfit(int []usedNumbers, String [] values, int size) {
    	int totalWeight = 0;
    	int i;

    	for(i=0; i<size; i++) {
    		if(usedNumbers[i] == 1) 
    			totalWeight = totalWeight + Integer.parseInt(values[i]);
    	}	
    	
        return totalWeight;
    }

    public static String calculateItems(int [] usedNumbers, int size) { 
		int i;
		String result = new String();
		result = "";
	
		for(i=0; i<size; i++) {
		    if(usedNumbers[i] == 1) 
		    	result = result.concat(String.valueOf(i)).concat(" ");
		}	
        return result;
    }

    // Return a range from the three biggest values
    public static int[] max(int[] t) {
    	int [] maximum = new int[3];   
    	maximum[0] = 0;
    	maximum[1] = 0;
    	maximum[2] = 0;

    	for (int i=1; i<t.length; i++) {
		    if (t[maximum[0]] < t[i]) {
				maximum[2] = maximum[1];
				maximum[1] = maximum[0];
				maximum[0] = i;
			 } else if (t[maximum[1]] < t[i]) {
		        maximum[2] = maximum[1];
		        maximum[1] = i;
		    } else if (t[maximum[2]] < t[i]) {
		        maximum[2] = i;
		    }
    	}
        return maximum;
    }
} // end class 

    
    
 /** Reduce Phase **/        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        Text result = new Text();

        for (Text val : values) {
        	result = val;    
        }

        context.write(key, result);    
    }
 }
 
 /** Main **/       
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }        
}

