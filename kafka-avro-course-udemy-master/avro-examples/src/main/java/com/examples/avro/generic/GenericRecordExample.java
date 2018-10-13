package com.examples.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static com.examples.avro.generic.Constants.*;

public class GenericRecordExample {

    public static void main(String[] args) {
        GenericRecordExample genericRecordExample = new GenericRecordExample();
        genericRecordExample.writeAndReadAvroFile();
    }

    private void writeAndReadAvroFile() {

        // step 0: create schema
        Schema.Parser parser = new Schema.Parser();
        String filePath = Objects.requireNonNull(getClass().getClassLoader().getResource("avro/customer.avsc")).getFile();
        Schema schema = null;

        try {
            schema = parser.parse(new File(filePath));
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }


        // step 1: generate record(s)
        CustomerConfig customerOne = new CustomerConfig("Yogesh", "Walia", 29, 5.11f, 95.5f);
        GenericRecord genericRecordOne = getGenericRecord(customerOne, schema);

        CustomerConfig customerTwo = new CustomerConfig("Rakesh", "Sharma", 28, 4.11f, 85.5f);
        customerTwo.setAutomatedEmail(false);
        GenericRecord genericRecordTwo = getGenericRecord(customerTwo, schema);

        CustomerConfig customerThree = new CustomerConfig("Vipin", "Singh", 27, 3.11f, 75.5f);
        GenericRecord genericRecordThree = getGenericRecord(customerThree, schema);


        // step 2: write record to avro file
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {

            dataFileWriter.create(schema, new File("3-customers.avro"));

            dataFileWriter.append(genericRecordOne);
            dataFileWriter.append(genericRecordTwo);
            dataFileWriter.append(genericRecordThree);

            System.out.println("Written Avro File Successfully !");
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }


        // step 3 and 4: read record from avro file and interpret the same
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        try (DataFileReader<GenericRecord> dataFileReader =
                     new DataFileReader<>(new File("3-customers.avro"), datumReader)) {

            System.out.println("\n----- Schema ----- : " + dataFileReader.getSchema() + "\n");

            int count = 1;
            while (dataFileReader.hasNext()) {

                System.out.println("Customer - " + count);

                GenericRecord customer = dataFileReader.next();
                System.out.println("First Name = " + customer.get(FIRST_NAME));
                System.out.println("Last Name = " + customer.get(LAST_NAME));
                System.out.println("Age = " + customer.get(AGE));
                System.out.println("Height in cm = " + customer.get(HEIGHT));
                System.out.println("Weight in kg = " + customer.get(WEIGHT));
                System.out.println("Automated Email = " + customer.get(AUTOMATED_EMAIL));

                System.out.println();
                count++;
            }

            System.out.println("Read Successfully !");
        } catch (IOException ioe) {
            System.out.println(ioe.getMessage());
        }
    }

    private GenericRecord getGenericRecord(CustomerConfig customer, Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set(FIRST_NAME, customer.getFirstName());
        builder.set(LAST_NAME, customer.getLastName());
        builder.set(AGE, customer.getAge());
        builder.set(HEIGHT, customer.getHeight());
        builder.set(WEIGHT, customer.getWeight());
        builder.set(AUTOMATED_EMAIL, customer.isAutomatedEmail());

        return builder.build();
    }
}
