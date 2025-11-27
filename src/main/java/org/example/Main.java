package org.example;

public class Main {
    public static void main(String[] args) {
        try {
            DataStreamJob.main(args);
        } catch (Exception e) {
            e.fillInStackTrace();
        }
    }
}