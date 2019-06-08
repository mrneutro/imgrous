package it.unisa.di.soa2019;

import java.io.*;
import java.util.*;

public class SequentialSimilarity {
    private final int ITER_SIZE = Integer.MAX_VALUE;
    private Map<Long, Map<String, Long>> mapPartitioning;
    private Map<Long, Map<String, Double>> mapSimilarity;
    private FileInputStream inputStreamIn = null;
    private Scanner scIn = null;
    private FileInputStream inputStreamOut = null;
    private Scanner scOut = null;
    private BufferedReader filePart = null;
    private String inPath, outPath, partPath;
    private Double norma;


    public static void main(String[] args) throws IOException {
        SequentialSimilarity sequentialSimilarity = new SequentialSimilarity();
        sequentialSimilarity.setup(args);
        sequentialSimilarity.partitioning();
        sequentialSimilarity.similarity();
    }

    public void calcLine(Long groupID, String[] words) {
        Map<String, Long> mapGroup = mapPartitioning.get(groupID);
        for (String word : words) {
            if (mapGroup.containsKey(word)) {
                mapGroup.put(word, mapGroup.get(word) + 1);
            } else {
                mapGroup.put(word, new Long(1));
            }
        }
        mapPartitioning.put(groupID, mapGroup);
    }

    public void partitioning() throws IOException {
        String[] values;
        int iter = 1;
        while (scIn.hasNextLine()) {
            boolean validLine = true;
            String line = scIn.nextLine();
            values = line.split(",", 5);
            String[] words = values[3].replaceAll("[^\b a-zA-Z0-9]", "").toLowerCase().split("\\s+");
            String group_id = values[2];
            if (words != null && words.length >= 1 && !words[0].isEmpty()) {
                try {
                    Long.parseLong(group_id);
                } catch (NumberFormatException e) {
                    validLine = false;
                }

                if (validLine) {
                    if (!mapPartitioning.containsKey(Long.parseLong(group_id))) {
                        mapPartitioning.put(Long.parseLong(group_id), new HashMap<String, Long>());
                    }
                    calcLine(Long.parseLong(group_id), words);
                    if ((iter % ITER_SIZE) == 0) {
                        updatePartFile();
                    }
                    iter++;
                }
            }
        }
        updatePartFile();
    }

    public void updatePartFile() throws IOException {
        String[] values;
        String line;
        StringBuffer inputBuffer = new StringBuffer();
        List<Long> groupInFile = new ArrayList<>();

        filePart = new BufferedReader(new FileReader(partPath));

        while ((line = filePart.readLine()) != null) {
            norma = new Double(0);
            values = line.split(",");
            Long groupID = Long.parseLong(values[0]);
            groupInFile.add(groupID);

            if (mapPartitioning.containsKey(groupID)) {
                Map<String, Long> lineMap = new HashMap<>();
                for (int i = 1; i < values.length - 1; i++) {
                    lineMap.put(values[i].split(":")[0], Long.parseLong(values[i].split(":")[1]));
                }

                Map<String, Long> mapWords = mapPartitioning.get(groupID);
                Set<String> words = mapWords.keySet();
                for (String word : words) {
                    if (lineMap.containsKey(word)) {
                        lineMap.put(word, mapWords.get(word) + lineMap.get(word));
                    } else {
                        lineMap.put(word, new Long(1));
                    }
                }
                StringBuilder lineBuilder = new StringBuilder();
                lineBuilder.append(groupID);
                for (Map.Entry<String, Long> entry : lineMap.entrySet()) {
                    lineBuilder.append(",").append(entry.getKey()).append(":").append(entry.getValue());
                    norma += Math.pow(entry.getValue(), 2);
                }
                norma = Math.sqrt(norma);
                lineBuilder.append(",").append(norma);
                line = lineBuilder.toString();
            }
            inputBuffer.append(line);
            inputBuffer.append('\n');
        }
        for (Long groupID : mapPartitioning.keySet()) {
            norma = new Double(0);
            if (!groupInFile.contains(groupID)) {
                StringBuilder lineBuilder = new StringBuilder();
                lineBuilder.append(groupID);
                Map<String, Long> mapGroup = mapPartitioning.get(groupID);
                Set<String> words = mapGroup.keySet();
                for (String word : words) {
                    lineBuilder.append(",").append(word).append(":").append(mapGroup.get(word));
                    norma += Math.pow(mapGroup.get(word), 2);
                }
                norma = Math.sqrt(norma);
                lineBuilder.append(",").append(norma);
                line = lineBuilder.toString();
                inputBuffer.append(line);
                inputBuffer.append('\n');
            }
        }
        filePart.close();
        String inputStr = inputBuffer.toString();

        FileOutputStream filePartOut = new FileOutputStream(partPath);
        filePartOut.write(inputStr.getBytes());
        filePartOut.close();
        mapPartitioning = new HashMap<>();
    }

    public void similarity() throws IOException {
        String[] values;
        String line;
        StringBuffer inputBuffer = new StringBuffer();
        mapSimilarity = new HashMap<>();

        filePart = new BufferedReader(new FileReader(partPath));

        while ((line = filePart.readLine()) != null) {
            values = line.split(",");
            Long groupID = Long.parseLong(values[0]);
            Map<String, Double> mapWords = new HashMap<>();

            for (int i = 1; i < values.length - 1; i++) {
                mapWords.put(values[i].split(":")[0], Double.parseDouble(values[i].split(":")[1]) / Double.parseDouble(values[values.length - 1]));
            }

            mapSimilarity.put(groupID, mapWords);
        }
        filePart.close();

        Set<Long> groupIDs = mapSimilarity.keySet();
        Iterator<Long> iteratorI = mapSimilarity.keySet().iterator();
        Iterator<Long> iteratorJ = mapSimilarity.keySet().iterator();
        for (int i = 0; i < groupIDs.size(); i++) {
            Long groupI = iteratorI.next();
            for (int j = 0; j <= i; j++) {
                iteratorJ.next();
            }
            for (int j = i + 1; j < groupIDs.size(); j++) {
                Long groupJ = iteratorJ.next();
                Double similarity = new Double(0);
                Map<String, Double> mapI = mapSimilarity.get(groupI);
                Map<String, Double> mapJ = mapSimilarity.get(groupJ);
                Iterable<String> wordsI = mapI.keySet();

                StringBuilder lineBuilder = new StringBuilder();
                lineBuilder.append("(").append(groupI).append(",").append(groupJ).append(")").append("\t");

                for (String word : wordsI) {
                    if (mapJ.containsKey(word)) {
                        similarity += mapI.get(word) * mapJ.get(word);
                    }
                }
                if (similarity != 0) {
                    lineBuilder.append(similarity);
                    line = lineBuilder.toString();
                    inputBuffer.append(line);
                    inputBuffer.append('\n');
                }
            }
            iteratorJ = mapSimilarity.keySet().iterator();
        }

        String inputStr = inputBuffer.toString();

        FileOutputStream fileSimOut = new FileOutputStream(outPath);
        fileSimOut.write(inputStr.getBytes());
        fileSimOut.close();
    }


    public void setup(String[] args) {
        inPath = args[0];
        outPath = args[1];
        partPath = args[2];
        try {
            mapPartitioning = new HashMap<>();
            File inPathFile = new File(inPath);
            inPathFile.createNewFile();
            inputStreamIn = new FileInputStream(inPath);
            scIn = new Scanner(inputStreamIn, "UTF-8");
            inputStreamOut = new FileInputStream(outPath);
            scOut = new Scanner(inputStreamOut, "UTF-8");
            File filePart = new File(partPath);
            filePart.createNewFile();
            PrintWriter writer = new PrintWriter(filePart);
            writer.print("");
            writer.close();
            File fileSim = new File(outPath);
            fileSim.createNewFile();
            writer = new PrintWriter(fileSim);
            writer.print("");
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
