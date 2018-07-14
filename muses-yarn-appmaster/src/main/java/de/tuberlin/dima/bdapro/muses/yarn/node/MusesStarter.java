package de.tuberlin.dima.bdapro.muses.yarn.node;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

///just for example.
public class MusesStarter {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello from Muses Start point");
        String confFilePath = args[0].substring(args[0].indexOf(":")+1);
        System.out.println("Path: " + args[0]);
        Files.lines(Paths.get(confFilePath.trim())).forEach(x -> System.out.println(x));
    }
}