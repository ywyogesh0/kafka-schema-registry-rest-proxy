package com.examples.avro.generic;

public class CustomerConfig {

    private String firstName;
    private String lastName;
    private int age;
    private float height;
    private float weight;
    private boolean automatedEmail = true;

    public CustomerConfig(String firstName, String lastName, int age, float height, float weight) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.height = height;
        this.weight = weight;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }

    public float getHeight() {
        return height;
    }

    public float getWeight() {
        return weight;
    }

    public boolean isAutomatedEmail() {
        return automatedEmail;
    }

    public void setAutomatedEmail(boolean automatedEmail) {
        this.automatedEmail = automatedEmail;
    }
}
