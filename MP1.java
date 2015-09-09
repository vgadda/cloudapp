
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class MP1 {
    Random generator;
    String userName;
    String inputFileName;
    String delimiters = " \t,;.?!-:@[](){}_*/";
    String[] stopWordsArray = {"i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
            "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its",
            "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
            "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having",
            "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while",
            "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before",
            "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again",
            "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each",
            "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than",
            "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"};

    void initialRandomGenerator(String seed) throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA");
        messageDigest.update(seed.toLowerCase().trim().getBytes());
        byte[] seedMD5 = messageDigest.digest();

        long longSeed = 0;
        for (int i = 0; i < seedMD5.length; i++) {
            longSeed += ((long) seedMD5[i] & 0xffL) << (8 * i);
        }

        this.generator = new Random(longSeed);
    }

    Integer[] getIndexes() throws NoSuchAlgorithmException {
        Integer n = 10000;
        Integer number_of_lines = 50000;
        Integer[] ret = new Integer[n];
        this.initialRandomGenerator(this.userName);
        for (int i = 0; i < n; i++) {
            ret[i] = generator.nextInt(number_of_lines);
        }
        return ret;
    }

    public MP1(String userName, String inputFileName) {
        this.userName = userName;
        this.inputFileName = inputFileName;
    }

    public String[] process() throws Exception {
        String[] ret = new String[20];
        Integer[] inputIdxs = getIndexes();
        Map<String, KeyValuepair> wordFreq = new HashMap<String, KeyValuepair>();
        Map<Integer, String> fileMap = getIndexedLines();
        for(int idx: inputIdxs){
        	for(String validWord: validWords(fileMap.get(idx))){
        		if(wordFreq.get(validWord) == null){
        			wordFreq.put(validWord, new KeyValuepair(validWord, 1));
        		}else{
        			KeyValuepair pair = wordFreq.get(validWord) ;
        			pair.incrementValue();
        			wordFreq.put(validWord, pair);
        		}
        	}
        }
        Set<KeyValuepair> sortedPair = new TreeSet<KeyValuepair>(wordFreq.values());
        int idx=0;
        for(Iterator<KeyValuepair> iter= sortedPair.iterator(); iter.hasNext();){
        	ret[idx] = iter.next().key;
        	idx++;
        	if(idx == 20)
        		break;
        	
        }
        writeToFile(ret);
        return ret;
    }
    
    class KeyValuepair implements Comparable<KeyValuepair>{
    	
    	private String key;
    	private Integer value;
    	
    	public KeyValuepair(String key, Integer value){
    		this.key = key;
    		this.value = value;
    	}
    	
		
		public int compareTo(KeyValuepair o2) {
			return value-o2.value == 0 ? key.compareTo(o2.key): o2.value-value;
		}
    	
		public void incrementValue(){
			value = value+1;
		}
		
		public String toString(){
			return key+":"+value;
		}
    }
    
    private void writeToFile(String[] lines) throws Exception{
    	BufferedWriter writer = new BufferedWriter(new FileWriter("./output.txt"));
    	try{
    		for(String line: lines){
    			writer.write(line);
    			writer.newLine();
    		}
    	}finally{
    		writer.close();
    	}
    }
        
    private Map<Integer, String> getIndexedLines() throws Exception{
    	BufferedReader reader = new BufferedReader(new FileReader(this.inputFileName)); 
        Map<Integer, String> fileMap = new HashMap<Integer, String>();
        try{
        	String line = reader.readLine();
        	
        	int currentPosition = 0;
        	while(line != null){
        		fileMap.put(currentPosition, line);       		
        		currentPosition++;
        		line = reader.readLine();
        	}
        }finally{
        	reader.close();
        }
        return fileMap;
    }
    
    private List<String> validWords(String line){
    	List<String> validWords = new ArrayList<String>();
    	StringTokenizer tokenizer = new StringTokenizer(line, delimiters);
    	while(tokenizer.hasMoreTokens()){
    		String lowerWord = tokenizer.nextToken().toLowerCase();
    		if(!isStopWord(lowerWord))
    			validWords.add(lowerWord);
    	}
    	return validWords;
    }
    
    private boolean isStopWord(String word){
    	boolean isStopWord = false;
    	for(String stopWord: stopWordsArray){
    		if(stopWord.equalsIgnoreCase(word)){
    			isStopWord = true;
    			break;
    		}
    	}
    	return isStopWord;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1){
            System.out.println("MP1 <User ID>");
        }
        else {
            String userName = args[0];
            String inputFileName = "./input.txt";
            MP1 mp = new MP1(userName, inputFileName);
            String[] topItems = mp.process();
            for (String item: topItems){
                System.out.println(item);
            }
        }
    }
}
