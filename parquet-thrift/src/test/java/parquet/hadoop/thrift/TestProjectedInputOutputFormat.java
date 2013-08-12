/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop.thrift;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.junit.Test;

import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.api.ReadSupport;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;

import org.apache.thrift.TBase;

public class TestProjectedInputOutputFormat {
  private static final Log LOG = Log.getLog(TestProjectedInputOutputFormat.class);
  
  public static Person getPerson() {
    return new Person(
						new Name("Felix", "Neutatz"),
						0,
						"bob.roberts@example.com",
						Arrays.asList(new PhoneNumber("1234567890")));
  };

  public static AddressBook getAddressbook() {
	return new AddressBook(
        Arrays.asList(
            new Person(
                new Name("Bob", "Roberts"),
                0,
                "bob.roberts@example.com",
                Arrays.asList(new PhoneNumber("1234567890")))));
  };

  public static class MyMapperAddressbook extends Mapper<LongWritable, Text, Void, AddressBook> {

    public void run(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,AddressBook>.Context context) throws IOException, InterruptedException {
        AddressBook a = TestProjectedInputOutputFormat.getAddressbook();
        context.write(null, a);      
    }
  }
  
  public static class MyMapperPerson extends Mapper<LongWritable, Text, Void, Person> {

    public void run(org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Void,Person>.Context context) throws IOException, InterruptedException {
        Person p = TestProjectedInputOutputFormat.getPerson();
        context.write(null, p);
    }
  }

  public static class MyMapper2 extends Mapper<Void, TBase<?,?>, LongWritable, Text> {
    protected void map(Void key, TBase<?,?> value, Mapper<Void,TBase<?,?>,LongWritable,Text>.Context context) throws IOException ,InterruptedException {
      context.write(null, new Text(value.toString()));
    }

  }

  @Test
  public void testReadWrite() throws Exception {
    final Configuration conf = new Configuration();
    final Path inputPath = new Path("src/test/java/parquet/hadoop/thrift/TestProjectedInputOutputFormat.java");
    final Path addressbookPath = new Path("target/test/thrift/TestProjectedInputOutputFormat/parquet/addressbook");
	final Path personPath = new Path("target/test/thrift/TestProjectedInputOutputFormat/parquet/person");
    final Path outputPath = new Path("target/test/thrift/TestProjectedInputOutputFormat/out");
    final FileSystem fileSystem = addressbookPath.getFileSystem(conf);
    fileSystem.delete(addressbookPath, true);
	fileSystem.delete(personPath, true);
    fileSystem.delete(outputPath, true);
    {
      final Job job = new Job(conf, "write");

      // input not really used
      TextInputFormat.addInputPath(job, inputPath);
      job.setInputFormatClass(TextInputFormat.class);

      job.setMapperClass(TestProjectedInputOutputFormat.MyMapperAddressbook.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(ParquetThriftOutputFormat.class);
      ParquetThriftOutputFormat.setOutputPath(job, addressbookPath);
      ParquetThriftOutputFormat.setThriftClass(job, AddressBook.class);

      waitForJob(job);
    }
	{
      final Job job = new Job(conf, "write");

      // input not really used
      TextInputFormat.addInputPath(job, inputPath);
      job.setInputFormatClass(TextInputFormat.class);

      job.setMapperClass(TestProjectedInputOutputFormat.MyMapperPerson.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(ParquetThriftOutputFormat.class);
      ParquetThriftOutputFormat.setOutputPath(job, personPath);
      ParquetThriftOutputFormat.setThriftClass(job, Person.class);

      waitForJob(job);
    }
    {
      // test with projection
	  final String readProjectionSchema = "message AddressBook {\n" + 
				"  optional group persons {\n" + 
				"    repeated group persons_tuple {\n" + 
				"      required group name {\n" + 
				"        optional binary first_name;\n" + 
				"        optional binary last_name;\n" + 
				"      }\n" + 
				"      optional int32 id;\n" + 
				"    }\n" + 
				"  }\n" + 
				"}" +
				
				"|" + 
				
				"message Person {\n" +     		
				"      required group name {\n" + 
				"        optional binary first_name;\n" + 
				"        optional binary last_name;\n" + 
				"      }\n" + 
				"      optional int32 id;\n" + 
				"}";
				
	  conf.set(ReadSupport.PARQUET_READ_SCHEMA, readProjectionSchema);
	  
	  
	  
	  final Job job = new Job(conf, "read");
	  
	  MultipleInputs.addInputPath(job, addressbookPath, FirstInputFormat.class);
	  MultipleInputs.addInputPath(job, personPath, ParquetThriftInputFormat.class);  

      job.setMapperClass(TestProjectedInputOutputFormat.MyMapper2.class);
      job.setNumReduceTasks(0);

      job.setOutputFormatClass(TextOutputFormat.class);
      TextOutputFormat.setOutputPath(job, outputPath);

      waitForJob(job);
    }

    
	AddressBook a_expected = TestProjectedInputOutputFormat.getAddressbook().deepCopy();
    for (Person person: a_expected.getPersons()) {
    	person.unsetEmail();
    	person.unsetPhones();
    }
	
	Person p_expected = TestProjectedInputOutputFormat.getPerson().deepCopy();
	p_expected.unsetEmail();
    p_expected.unsetPhones();
	
	String lineOut = null;
	
	//check addressbook record
	final BufferedReader outAddressbook = new BufferedReader(new FileReader(new File(outputPath.toString(), "part-m-00000")));	
    lineOut = outAddressbook.readLine();
    lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
    assertEquals(a_expected.toString(), lineOut);
	outAddressbook.close();
	
	//check person record
	final BufferedReader outPerson = new BufferedReader(new FileReader(new File(outputPath.toString(), "part-m-00001")));	
    lineOut = outPerson.readLine();
    lineOut = lineOut.substring(lineOut.indexOf("\t") + 1);
    assertEquals(p_expected.toString(), lineOut);
	outPerson.close();
  }

  private void waitForJob(Job job) throws Exception {
    job.submit();
    while (!job.isComplete()) {
      LOG.debug("waiting for job " + job.getJobName());
      sleep(100);
    }
    LOG.info("status for job " + job.getJobName() + ": " + (job.isSuccessful() ? "SUCCESS" : "FAILURE"));
    if (!job.isSuccessful()) {
      throw new RuntimeException("job failed " + job.getJobName());
    }
  }

}
