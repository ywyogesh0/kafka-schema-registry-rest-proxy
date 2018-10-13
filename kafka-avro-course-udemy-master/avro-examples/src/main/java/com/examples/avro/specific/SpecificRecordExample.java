package com.examples.avro.specific;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {

    public static void main(String[] args) {
        SpecificRecordExample specificRecordExample = new SpecificRecordExample();
        specificRecordExample.writeAndReadAvroFile();
    }

    private void writeAndReadAvroFile() {

        // step 1: generate record(s)
        Customer.Builder builderOne = Customer.newBuilder();
        builderOne.setFirstName("Yogesh");
        builderOne.setLastName("Walia");
        builderOne.setAge(29);
        builderOne.setHeight(5.11f);
        builderOne.setWeight(95.5f);
        Customer customerOne = builderOne.build();

        Customer.Builder builderTwo = Customer.newBuilder();
        builderTwo.setFirstName("Rakesh");
        builderTwo.setLastName("Sharma");
        builderTwo.setAge(28);
        builderTwo.setHeight(4.11f);
        builderTwo.setWeight(85.5f);
        Customer customerTwo = builderTwo.build();

        Customer.Builder builderThree = Customer.newBuilder();
        builderThree.setFirstName("Vipin");
        builderThree.setLastName("Singh");
        builderThree.setAge(27);
        builderThree.setHeight(3.11f);
        builderThree.setWeight(75.5f);
        Customer customerThree = builderThree.build();


        // step 2: write record to avro file
        DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {

            dataFileWriter.create(Customer.getClassSchema(), new File("3-specific-customers.avro"));

            dataFileWriter.append(customerOne);
            dataFileWriter.append(customerTwo);
            dataFileWriter.append(customerThree);

            System.out.println("Written Avro File Successfully !");
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }


        // step 3 and 4: read record from avro file and interpret the same
        DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try (DataFileReader<Customer> dataFileReader =
                     new DataFileReader<>(new File("3-specific-customers.avro"), datumReader)) {

            System.out.println("\n----- Schema ----- : " + dataFileReader.getSchema() + "\n");

            int count = 1;
            while (dataFileReader.hasNext()) {

                System.out.println("Customer - " + count);

                Customer customer = dataFileReader.next();
                System.out.println("First Name = " + customer.getFirstName());
                System.out.println("Last Name = " + customer.getLastName());
                System.out.println("Age = " + customer.getAge());
                System.out.println("Height in cm = " + customer.getHeight());
                System.out.println("Weight in kg = " + customer.getWeight());
                System.out.println("Automated Email = " + customer.getAutomatedEmail());

                System.out.println();
                count++;
            }

            System.out.println("Read Successfully !");
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }
}
