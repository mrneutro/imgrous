package it.unisa.di.soa2019;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitioningRunnable implements Runnable {

    private long threadID;
    private boolean calcMap = false;
    private boolean mapCmp = false;
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
        mapCmp = true;
    }


    @Override
    public void run() {
        threadID = Thread.currentThread().getId();
//        System.out.println("Thread " + threadID % 4 + " running");
        try {
            if (calcMap) {
                main.callback(calcMap());
            }

            if (mapCmp) {
                mapCmp(mapPartitioning2);
                main.callback();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mapCmp(Map<Long, Map<String, Long>> mapPartitioning2) {
        Map<String, Long> mapGroup;

        for (Long groupID : mapPartitioning2.keySet()) {
            if (mapPartitioning.containsKey(groupID)) {
                mapGroup = mapPartitioning.get(groupID);
            } else {
                mapGroup = new HashMap<>();
                mapPartitioning.put(groupID, mapGroup);
            }
            for (String word : mapPartitioning2.get(groupID).keySet()) {
                if (mapGroup.containsKey(word)) {
                    if(mapGroup.get(word) != null)
                        mapGroup.put(word, mapGroup.get(word) + 1);
                } else {
                    mapGroup.put(word, new Long(1));
                }
            }
        }
    }

    private Map<Long, Map<String, Long>> calcMap() throws IOException {
        String line;
        String[] values;
        BufferedReader bufferedPart = new BufferedReader(new FileReader(main.getPartPath() + (threadID % main.getNumberOfProcessors())));

        while ((line = bufferedPart.readLine()) != null) {
            values = line.split(",");
            List<String> words = new ArrayList<>();
            for (int i = 1; i < values.length; i++) {
                words.add(values[i]);
            }
            Long groupID = Long.parseLong(values[0]);
            if(mapPartitioningList.containsKey(groupID)){
                List<String> toUpdate = mapPartitioningList.get(groupID);
                toUpdate.addAll(words);
            } else {
                mapPartitioningList.put(groupID, words);
            }
        }
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
}
