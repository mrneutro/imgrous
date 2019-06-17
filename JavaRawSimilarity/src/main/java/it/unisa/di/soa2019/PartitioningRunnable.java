package it.unisa.di.soa2019;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitioningRunnable implements Runnable {

    private long threadID;
    private boolean calcMap = false;
    private boolean mapCmp = false;
    private boolean mapCmp2 = false;
    private Main main;
    private Map<Long, List<String>> mapPartitioningList;
    private Map<Long, Map<String, Long>> mapPartitioning;
    private Map<Long, Map<String, Long>> mapPartitioning2;


    public PartitioningRunnable(Main main, Map<Long, List<String>> mapPartitioningList) {
        this.main = main;
        this.mapPartitioningList = mapPartitioningList;
        calcMap = true;
    }

    public PartitioningRunnable(Main main, Map<Long, Map<String, Long>> mapPartitioning, Map<Long, Map<String, Long>> mapPartitioning2) {
        this.main = main;
        this.mapPartitioning = mapPartitioning;
        this.mapPartitioning2 = mapPartitioning2;
        mapCmp2 = true;
    }

//    public PartitioningRunnable(Main main, Map<Long, Map<String, Long>> mapPartitioning1, Map<Long, Map<String, Long>> mapPartitioning2, String partPath) {
//        this.main = main;
//        this.mapPartitioning1 = mapPartitioning1;
//        this.mapPartitioning2 = mapPartitioning2;
//        this.partPath = partPath;
//        updatePartFile = true;
//    }


    @Override
    public void run() {
        threadID = Thread.currentThread().getId();
        System.out.println("Thread " + threadID % 4 + " running");
        try {
            if (calcMap) {
                main.callback(calcMap());
            }

            if (mapCmp) {
                mapCmp(mapPartitioning, mapPartitioning2);
                main.callback();
            }

            if (mapCmp2) {
                mapCmp(mapPartitioning2);
                main.callback();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//            if (updatePartFile){
//                mapPartitioning = mapCmp();
//                updatePartFile();
//            }
    }

//    private Map<Long, Map<String, Long>> mapCmp() {
//        Map<String, Long> mapGroup;
//
//        for (Long groupID : mapPartitioning1.keySet()) {
//            if (mapPartitioning2.containsKey(groupID)) {
//                mapGroup = mapPartitioning2.get(groupID);
//            } else {
//                mapGroup = new HashMap<>();
//                mapPartitioning2.put(groupID, mapGroup);
//            }
//            for (String word : mapPartitioning1.get(groupID).keySet()) {
//                if (mapGroup.containsKey(word)) {
//                    mapGroup.put(word, mapGroup.get(word) + 1);
//                } else {
//                    mapGroup.put(word, new Long(1));
//                }
//            }
//        }
//
//        return mapPartitioning2;
//    }

    private void mapCmp(Map<Long, Map<String, Long>> mapPartitioning2) {
        Map<String, Long> mapGroup;

        for (Long groupID : mapPartitioning2.keySet()) {
            synchronized (mapPartitioning){
                if (mapPartitioning.containsKey(groupID)) {
                    mapGroup = mapPartitioning.get(groupID);
                } else {
                    mapGroup = new HashMap<>();
                    mapPartitioning.put(groupID, mapGroup);
                }
                for (String word : mapPartitioning2.get(groupID).keySet()) {
                    if (mapGroup.containsKey(word)) {
                        mapGroup.put(word, mapGroup.get(word) + 1);
                    } else {
                        mapGroup.put(word, new Long(1));
                    }
                }
            }
        }
    }

    private void mapCmp(Map<Long, Map<String, Long>> mapPartitioning1, Map<Long, Map<String, Long>> mapPartitioning2) {
        Map<String, Long> mapGroup;

        for (Long groupID : mapPartitioning1.keySet()) {
            if (mapPartitioning2.containsKey(groupID)) {
                mapGroup = mapPartitioning2.get(groupID);
            } else {
                mapGroup = new HashMap<>();
                mapPartitioning2.put(groupID, mapGroup);
            }
            for (String word : mapPartitioning1.get(groupID).keySet()) {
                if (mapGroup.containsKey(word)) {
                    mapGroup.put(word, mapGroup.get(word) + 1);
                } else {
                    mapGroup.put(word, new Long(1));
                }
            }
        }

        for (Long groupID : mapPartitioning2.keySet()) {
            if (mapPartitioning.containsKey(groupID)) {
                mapGroup = mapPartitioning.get(groupID);
            } else {
                mapGroup = new HashMap<>();
                mapPartitioning.put(groupID, mapGroup);
            }
            for (String word : mapPartitioning2.get(groupID).keySet()) {
                if (mapGroup.containsKey(word)) {
                    mapGroup.put(word, mapGroup.get(word) + 1);
                } else {
                    mapGroup.put(word, new Long(1));
                }
            }
        }
    }

    private Map<Long, Map<String, Long>> calcMap() {
        mapPartitioning = new HashMap<>();
        Map<String, Long> mapGroup;
        for (Map.Entry<Long, List<String>> entry : mapPartitioningList.entrySet()) {
            if (mapPartitioning.containsKey(entry.getKey())) {
                mapGroup = mapPartitioning.get(entry.getKey());
            } else {
                mapGroup = new HashMap<>();
                mapPartitioning.put(entry.getKey(), mapGroup);
            }
            for (String word : entry.getValue()) {
                if (mapGroup.containsKey(word)) {
                    mapGroup.put(word, mapGroup.get(word) + 1);
                } else {
                    mapGroup.put(word, new Long(1));
                }
            }
        }
        return mapPartitioning;
    }


//    public void updatePartFile() throws IOException {
//        String[] values;
//        String line;
//        StringBuffer inputBuffer = new StringBuffer();
//        List<Long> groupInFile = new ArrayList<>();
//
//        filePart = new BufferedReader(new FileReader(partPath));
//
//        while ((line = filePart.readLine()) != null) {
//            norma = new Double(0);
//            values = line.split(",");
//            Long groupID = Long.parseLong(values[0]);
//            groupInFile.add(groupID);
//
//            if (mapPartitioning.containsKey(groupID)) {
//                Map<String, Long> lineMap = new HashMap<>();
//                for (int i = 1; i < values.length - 1; i++) {
//                    lineMap.put(values[i].split(":")[0], Long.parseLong(values[i].split(":")[1]));
//                }
//
//                Map<String, Long> mapWords = mapPartitioning.get(groupID);
//                Set<String> words = mapWords.keySet();
//                for (String word : words) {
//                    if (lineMap.containsKey(word)) {
//                        lineMap.put(word, mapWords.get(word) + lineMap.get(word));
//                    } else {
//                        lineMap.put(word, new Long(1));
//                    }
//                }
//                StringBuilder lineBuilder = new StringBuilder();
//                lineBuilder.append(groupID);
//                for (Map.Entry<String, Long> entry : lineMap.entrySet()) {
//                    lineBuilder.append(",").append(entry.getKey()).append(":").append(entry.getValue());
//                    norma += Math.pow(entry.getValue(), 2);
//                }
//                norma = Math.sqrt(norma);
//                lineBuilder.append(",").append(norma);
//                line = lineBuilder.toString();
//            }
//            inputBuffer.append(line);
//            inputBuffer.append('\n');
//        }
//        for (Long groupID : mapPartitioning.keySet()) {
//            norma = new Double(0);
//            if (!groupInFile.contains(groupID)) {
//                StringBuilder lineBuilder = new StringBuilder();
//                lineBuilder.append(groupID);
//                Map<String, Long> mapGroup = mapPartitioning.get(groupID);
//                Set<String> words = mapGroup.keySet();
//                for (String word : words) {
//                    lineBuilder.append(",").append(word).append(":").append(mapGroup.get(word));
//                    norma += Math.pow(mapGroup.get(word), 2);
//                }
//                norma = Math.sqrt(norma);
//                lineBuilder.append(",").append(norma);
//                line = lineBuilder.toString();
//                inputBuffer.append(line);
//                inputBuffer.append('\n');
//            }
//        }
//        filePart.close();
//        String inputStr = inputBuffer.toString();
//
//        FileOutputStream filePartOut = new FileOutputStream(partPath);
//        filePartOut.write(inputStr.getBytes());
//        filePartOut.close();
//        mapPartitioning = new HashMap<>();
//    }
}
