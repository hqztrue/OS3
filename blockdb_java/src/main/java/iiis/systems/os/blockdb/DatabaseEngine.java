package iiis.systems.os.blockdb;

import java.util.HashMap;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.json.JSONObject;
import org.json.JSONException;
import com.google.protobuf.util.JsonFormat;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DatabaseEngine {
    private static DatabaseEngine instance = null;

    public static DatabaseEngine getInstance() {
        return instance;
    }

    public static void setup(String dataDir) {
        instance = new DatabaseEngine(dataDir);
    }

    private HashMap<String, Integer> balances = new HashMap<>();
	private ReadWriteLock lock = new ReentrantReadWriteLock();
    private int logLength = 0, blockCnt = 0;
    private String dataDir;
	public final int N = 50;
	public String savedLogInfoPath = dataDir + "info.txt";
	Block.Builder block_builder = Block.newBuilder();
	
	private void saveLog(Transaction transaction){
		if (logLength == 0){
			++blockCnt;
			block_builder.setBlockID(blockCnt).setPrevHash("00000000").clearTransactions().setNonce("00000000");
		}
		block_builder.addTransactions(transaction);
		logLength = (logLength+1)%N;
		//File dataPath = new File(dataDir);
		try{
			filePath = dataDir + blockCnt + ".json";
			/*FileWriter fw = new FileWriter(filePath);
			PrintWriter out = new PrintWriter(fw);
			out.write();
			out.println();
			fw.close();
			out.close();*/
			//String jsonFormat =JsonFormat.printToString(block_builder.build());
			FileWriter writer = new FileWriter(filePath);
			JsonFormat.printer().appendTo(block_builder.build(), writer);
			writer.close();
			
			//JSONObject jObj = ;
			FileWriter fw = new FileWriter(savedLogInfoPath);
			BufferedWriter out = new BufferedWriter(fw);
			out.write(""+blockCnt+"\n"+logLength+"\n");
			fw.close();
			out.close();
		}
		catch (Exception e){
            e.printStackTrace();
        }
	}
	
	private boolean restoreLog(){
		lock.writeLock.lock();
		try{
			//JSONObject logInfoObj = Util.readJsonFile(savedLogInfoPath);
			//blockCnt = logInfoObj.getInt("blockCnt");
			//logLength = logInfoObj.getInt("logLength");
			File file = new File(savedLogInfoPath);
			if (file.exists()){
				FileReader fr = new FileReader(savedLogInfoPath);
				BufferedReader in = new BufferedReader(fr);
				blockCnt = Integer.parseInt(logReader.readLine()).intValue();
				logLength = Integer.parseInt(logReader.readLine()).intValue();
				fr.close();
				in.close();
			}
			else {
				blockCnt = logLength = 0;
			}
			
			for (int i=1;i<=blockCnt;++i){
				FileReader reader = new FileReader(dataDir + i + ".json");
				JsonFormat.parser().merge(reader, block_builder);
				int num = N;
				if (i==blockCnt)num = logLength;
				for (int j=0;j<num;++j){
					Transaction transaction = builder.getTransactions(j);
					Transaction.Types type = transaction.getType();
					String userId = transaction.getUserID();
					String fromId = transaction.getFromID();
					String toId = transaction.getToID();
					int value = transaction.getValue(), balance;
					switch (type){
						case Transaction.Types.PUT:
							balances.put(userId, value);
							break;
						case Transaction.Types.DEPOSIT:
							balance = getOrZero(userId);
							balances.put(userId, balance + value);
							break;
						case Transaction.Types.WITHDRAW:
							balance = getOrZero(userId);
							balances.put(userId, balance - value);
							break;
						case Transaction.Types.TRANSFER:
							int fromBalance = getOrZero(fromId);
							int toBalance = getOrZero(toId);
							balances.put(fromId, fromBalance - value);
							balances.put(toId, toBalance + value);
							break;
						default:
							lock.writeLock.unlock();
							return false;
				}
			}
		}
		catch (Exception e){
            e.printStackTrace();
			lock.writeLock.unlock();
			return false;
        }
		lock.writeLock.unlock();
		return true;
	}
	
    DatabaseEngine(String dataDir) {
        this.dataDir = dataDir;
		this.savedLogInfoPath = this.dataDir + "info.txt";
		//restoreLog();
    }

    private int getOrZero(String userId) {
        if (balances.containsKey(userId)) {
            return balances.get(userId);
        } else {
            return 0;
        }
    }

    public int get(String userId) {
        //logLength++;
		lock.readLock.lock();
		try{
			int ans = getOrZero(userId);
		}
		catch (Exception e){
            e.printStackTrace();
        }
		lock.readLock.unlock();
        return ans;
    }

    public boolean put(String userId, int value) {
		if (value<0)return false;
		lock.writeLock.lock();
		try{
			balances.put(userId, value);
		
			Transaction.Builder transaction_builder = Transaction.newBuilder();
			Transaction transaction = transaction_builder.setType(Transaction.Types.PUT).setUserID(userId).setValue(value).build();
			saveLog(transaction);
		}
		catch (Exception e){
            e.printStackTrace();
        }
		lock.writeLock.unlock();
        return true;
    }

    public boolean deposit(String userId, int value) {
		if (value<0)return false;
        lock.writeLock.lock();
		try{
			int balance = getOrZero(userId);
			balances.put(userId, balance + value);
			
			Transaction.Builder transaction_builder = Transaction.newBuilder();
			Transaction transaction = transaction_builder.setType(Transaction.Types.DEPOSIT).setUserID(userId).setValue(value).build();
			saveLog(transaction);
		}
		catch (Exception e){
            e.printStackTrace();
        }
		lock.writeLock.unlock();
        return true;
    }

    public boolean withdraw(String userId, int value) {
		if (value<0)return false;
		lock.writeLock.lock();
		try{
			int balance = getOrZero(userId);
			if (balance < value){
				lock.writeLock.unlock();
				return false;
			}
			balances.put(userId, balance - value);
			
			Transaction.Builder transaction_builder = Transaction.newBuilder();
			Transaction transaction = transaction_builder.setType(Transaction.Types.WITHDRAW).setUserID(userId).setValue(value).build();
			saveLog(transaction);
		}
		catch (Exception e){
            e.printStackTrace();
        }
		lock.writeLock.unlock();
        return true;
    }

    public boolean transfer(String fromId, String toId, int value) {
		if (value<0 || fromId.equals(toId))return false;
		lock.writeLock.lock();
		try{
			int fromBalance = getOrZero(fromId);
			if (fromBalance < value){
				lock.writeLock.unlock();
				return false;
			}
			int toBalance = getOrZero(toId);
			balances.put(fromId, fromBalance - value);
			balances.put(toId, toBalance + value);
			
			Transaction.Builder transaction_builder = Transaction.newBuilder();
			Transaction transaction = transaction_builder.setType(Transaction.Types.TRANSFER).setFromID(fromId).setToID(toId).setValue(value).build();
			saveLog(transaction);
		}
		catch (Exception e){
            e.printStackTrace();
        }
		lock.writeLock.unlock();
        return true;
    }

    public int getLogLength() {
		lock.readLock.lock();
        int ans = logLength;
		lock.readLock.unlock();
		return ans;
    }
}
