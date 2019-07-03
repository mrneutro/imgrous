package it.unisa.di.soa2019;

import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;
import org.tartarus.snowball.ext.italianStemmer;
import org.tartarus.snowball.ext.russianStemmer;
import org.tartarus.snowball.ext.spanishStemmer;

import java.io.*;
import java.util.*;

public class PartitioningRunnable implements Runnable {

    private long threadID;
    private boolean calcMap = false;
    private boolean mapCmp = false;
    private Main main;
    private List<List<String>> wordGroupList;
    private Map<Long, List<String>> mapPartitioningList;
    private Map<Long, Map<String, Long>> mapPartitioning;
    private Map<Long, Map<String, Long>> mapPartitioning2;


    public PartitioningRunnable(Main main, List<List<String>> wordGroupList) {
        this.main = main;
        this.wordGroupList = wordGroupList;
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
                    if (mapGroup.get(word) != null)
                        try {
                            if(groupID.equals(new Long(1096901835)) && word.equals("sa"))
                                System.out.println(word);
                            mapGroup.put(word, mapGroup.get(word) + mapPartitioning2.get(groupID).get(word));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                } else {
                    mapGroup.put(word, mapPartitioning2.get(groupID).get(word));
                }
            }
        }
    }

    private Map<Long, Map<String, Long>> calcMap() throws IOException {
        String line;
        String[] values;
        BufferedReader bufferedPart = new BufferedReader(new FileReader(main.getPartPath() + (threadID % main.getNumberOfProcessors())));
        Map<String, Long> mapGroup;
        mapPartitioning = new HashMap<>();

        HashMap<String, SnowballStemmer> stemmersMap;
        SnowballStemmer genericStemmer = new englishStemmer();

        stemmersMap = new HashMap<>(4);
        stemmersMap.put("it", new italianStemmer());
        stemmersMap.put("ru", new russianStemmer());
        stemmersMap.put("es", new spanishStemmer());
        stemmersMap.put("en", new englishStemmer());

        Map<Integer, Map<Long, List<String>>> mapPartitioningLists = new HashMap<>();
        for (int i = 0; i < main.getNumberOfProcessors(); i++) {
            mapPartitioningLists.put(i, (Map) new HashMap<>());
        }

        for (List<String> wordList : wordGroupList) {
            Long groupID = null;
            String lang = wordList.get(1);
            try {
                groupID = Long.parseLong(String.valueOf(wordList.get(0)));
            } catch (NumberFormatException e) {
                e.printStackTrace();
                continue;
            }

            if (mapPartitioning.containsKey(groupID)) {
                mapGroup = mapPartitioning.get(groupID);
            } else {
                mapGroup = new HashMap<>();
                mapPartitioning.put(groupID, mapGroup);
            }

            wordList = wordList.subList(2, wordList.size());

            for (String word : wordList) {
                if (word.length() == 0) {
                    continue;
                }

                SnowballStemmer currentStemmer = null;
                if (stemmersMap.containsKey(lang)) {
                    currentStemmer = stemmersMap.get(lang);
                } else {
                    currentStemmer = genericStemmer;
                }
                if (StopWords.getInstance().isStopWord(word)) {
                    continue;
                }
                currentStemmer.setCurrent(word);
                currentStemmer.stem();
                String stemmed = currentStemmer.getCurrent();

                if (mapGroup.containsKey(stemmed)) {
                    mapGroup.put(stemmed, mapGroup.get(stemmed) + 1);
                } else {
                    mapGroup.put(stemmed, new Long(1));
                }
            }
        }

        while ((line = bufferedPart.readLine()) != null) {

            boolean validLine = true;
            values = line.split(",");
            List<String> words;

            if(values.length > 2) {

                Long groupID = null;
                try {
                    groupID = Long.parseLong(values[0]);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                    continue;
                }

                String lang;
                lang = values[1];
                for (int i = 3; i < values.length ; i++) {
                    values[2] = values[2] + "," + values[i]; // TODO use string builder here
                }

                words = Arrays.asList(values[2].split(","));
                List<String> wordList = new ArrayList<>();

                if (words.size() >= 1) {
                    if (validLine) {
                        for (String word : words) {
                            if (word.length() == 0) {
                                continue;
                            }

                            SnowballStemmer currentStemmer = null;
                            if (stemmersMap.containsKey(lang)) {
                                currentStemmer = stemmersMap.get(lang);
                            } else {
                                currentStemmer = genericStemmer;
                            }
                            if (StopWords.getInstance().isStopWord(word)) {
                                continue;
                            }
                            currentStemmer.setCurrent(word);
                            currentStemmer.stem();
                            String stemmed = currentStemmer.getCurrent();
                            wordList.add(stemmed);
                        }
                    }
                }

                if (mapPartitioning.containsKey(groupID)) {
                    mapGroup = mapPartitioning.get(groupID);
                } else {
                    mapGroup = new HashMap<>();
                    mapPartitioning.put(groupID, mapGroup);
                }

                for (String word : wordList) {
                    if (mapGroup.containsKey(word)) {
                        mapGroup.put(word, mapGroup.get(word) + 1);
                    } else {
                        mapGroup.put(word, new Long(1));
                    }
                }
            }
        }
        return mapPartitioning;
    }
}
