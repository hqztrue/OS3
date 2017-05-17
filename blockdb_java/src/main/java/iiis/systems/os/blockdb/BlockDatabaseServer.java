package iiis.systems.os.blockdb;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.*;

import java.io.IOException;
import java.net.InetSocketAddress;

public class BlockDatabaseServer {
    private Server server;

    private void start(String address, int port) throws IOException {
        server = NettyServerBuilder.forAddress(new InetSocketAddress(address, port))
                .addService(new BlockDatabaseImpl())
                .build()
                .start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                BlockDatabaseServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

	public static void test1(){
		DatabaseEngine.setup("./test/");
		final DatabaseEngine engine = DatabaseEngine.getInstance();
		for (int i=0; i<10; ++i){
			System.out.println(""+i+"deposit"+engine.deposit(Integer.toString(i), 1));
		}
		for (int i=0; i<5; ++i){
			System.out.println(""+i+"deposit"+engine.deposit(Integer.toString(i), 1));
		}
		for (int i=0; i<10; ++i){
			System.out.println(""+i+"get"+engine.get(Integer.toString(i)));
		}
		for (int i=0; i<4; ++i){
			System.out.println(""+i+"put"+engine.put(Integer.toString(i), i));
		}
		for (int i=0; i<10; ++i){
			System.out.println(""+i+"get"+engine.get(Integer.toString(i)));
		}
		for (int i=0; i<10; ++i){
			System.out.println(""+i+"withdraw"+engine.withdraw(Integer.toString(i), 1));
		}
		for (int i=11; i>0; --i)
			if (i%2==1){
				System.out.println(""+i+"transfer"+engine.transfer(Integer.toString(i), Integer.toString(i-1), 2));
			}
		for (int i=0; i<10; ++i){
			System.out.println(""+i+"get"+engine.get(Integer.toString(i)));
		}
	}
	
	public static void test2(){
		DatabaseEngine.setup("./test/");
        final DatabaseEngine engine = DatabaseEngine.getInstance();
		final int T = 1;
		Thread t1 = new Thread(new Runnable(){
			public void run(){
				for (int i=0; i<10; ++i){
					System.out.println(""+i+"deposit"+engine.deposit(Integer.toString(i), 1));
                    try{Thread.sleep(T);}catch (Exception e){}
				}
				for (int i=0; i<10; ++i){
					System.out.println(""+i+"get"+engine.get(Integer.toString(i)));
					try{Thread.sleep(T);}catch (Exception e){}
				}
			}
		});
		Thread t2 = new Thread(new Runnable(){
			public void run(){
				for (int i=0; i<5; ++i){
					System.out.println(""+i+"deposit"+engine.deposit(Integer.toString(i), 1));
					try{Thread.sleep(T);}catch (Exception e){}
				}
			}
		});
		Thread t3 = new Thread(new Runnable(){
			public void run(){
				for (int i=0; i<4; ++i){
					System.out.println(""+i+"put"+engine.put(Integer.toString(i), i));
					try{Thread.sleep(T);}catch (Exception e){}
				}
				for (int i=0; i<10; ++i){
					System.out.println(""+i+"get"+engine.get(Integer.toString(i)));
					try{Thread.sleep(T);}catch (Exception e){}
				}
			}
		});
		Thread t4 = new Thread(new Runnable(){
			public void run(){
				for (int i=0; i<10; ++i){
					System.out.println(""+i+"withdraw"+engine.withdraw(Integer.toString(i), 1));
					try{Thread.sleep(T);}catch (Exception e){}
				}
				for (int i=11; i>0; --i)
					if (i%2==1){
						System.out.println(""+i+"transfer"+engine.transfer(Integer.toString(i), Integer.toString(i-1), 2));
						try{Thread.sleep(T);}catch (Exception e){}
					}
				for (int i=0; i<10; ++i){
					System.out.println(""+i+"get"+engine.get(Integer.toString(i)));
					try{Thread.sleep(T);}catch (Exception e){}
				}
			}
		});
		t1.start();
		t2.start();
		t3.start();
		t4.start();
        try{t1.join();t2.join();t3.join();t4.join();}catch (Exception e){}
	}
	
	public static void test3(){
		DatabaseEngine.setup("./test/");
        final DatabaseEngine engine = DatabaseEngine.getInstance();
		final int T = 100;
		for (int i=0; i<10; ++i){
			System.out.println(""+i+"put"+engine.put(Integer.toString(i), i));
		}
		Thread t1 = new Thread(new Runnable(){
			public void run(){
				for (int j=0;j<20;++j){
					for (int i=0;i<10;++i){
						System.out.println(""+i+"get"+engine.get(Integer.toString(i)));
					}
                    System.out.println("-------------");
					try{Thread.sleep(T);}catch (Exception e){}
				}
			}
		});
		Thread t2 = new Thread(new Runnable(){
			public void run(){
				java.util.Random ran=new java.util.Random(); 
				for (int i=0; i<10; ++i){
                    int from = (ran.nextInt()%10+10)%10, to = (ran.nextInt()%10+10)%10;
					System.out.println("transfer"+from+" "+to+" "+engine.transfer(Integer.toString(from), Integer.toString(to), 1));
					try{Thread.sleep(T);}catch (Exception e){}
				}
			}
		});
		t1.start();
		t2.start();
        try{t1.join();t2.join();}catch (Exception e){}
	}
	
    public static void test0(){
        try{
            JSONObject obj1 = Util.readJsonFile("/Users/lwz/Downloads/OS3-master/blockdb_go/example/example_1.json");
            JSONObject obj2 = Util.readJsonFile("/Users/lwz/Downloads/OS3-master/blockdb_java/tmp/1.json");
            System.out.println(jsonsEqual(obj1,obj2));
        }
        catch(Exception e){
        }
    }
    
    public static boolean jsonsEqual(Object obj1, Object obj2) throws JSONException{
        if (!obj1.getClass().equals(obj2.getClass()))return false;
        if (obj1 instanceof JSONObject){
            JSONObject jsonObj1 = (JSONObject) obj1;
            JSONObject jsonObj2 = (JSONObject) obj2;
            String[] names = JSONObject.getNames(jsonObj1);
            String[] names2 = JSONObject.getNames(jsonObj1);
            if (names.length != names2.length)return false;
            for (String fieldName:names){
                Object obj1FieldValue = jsonObj1.get(fieldName);
                Object obj2FieldValue = jsonObj2.get(fieldName);
                if (!jsonsEqual(obj1FieldValue, obj2FieldValue))return false;
            }
        }
        else if (obj1 instanceof JSONArray){
            JSONArray obj1Array = (JSONArray) obj1;
            JSONArray obj2Array = (JSONArray) obj2;
            if (obj1Array.length() != obj2Array.length())return false;
            int flag[]=new int[obj1Array.length()];
            for (int i = 0; i < obj1Array.length(); i++)flag[i]=0;
            for (int i = 0; i < obj1Array.length(); i++){
                boolean matchFound = false;
                for (int j = 0; j < obj2Array.length(); j++){
                    if (flag[j]==0 && jsonsEqual(obj1Array.get(i), obj2Array.get(j))){
                        matchFound = true;
                        flag[j]=1;
                        break;
                    }
                }
                if (!matchFound)return false;
            }
        }
        else {
            if (!obj1.equals(obj2))return false;
        }
        return true;
    }
    
    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
        //System.out.println("test1");test1();return;
		//System.out.println("test2");test2();return;
		//System.out.println("test3");test3();return;
		//System.out.println("test0");test0();return;
        
		JSONObject config = Util.readJsonFile("config.json");
        config = (JSONObject)config.get("1");
        String address = config.getString("ip");
        int port = Integer.parseInt(config.getString("port"));
        String dataDir = config.getString("dataDir");

        DatabaseEngine.setup(dataDir);

        final BlockDatabaseServer server = new BlockDatabaseServer();
        server.start(address, port);
        server.blockUntilShutdown();
    }

    static class BlockDatabaseImpl extends BlockDatabaseGrpc.BlockDatabaseImplBase {
        private final DatabaseEngine dbEngine = DatabaseEngine.getInstance();

        @Override
        public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.get(request.getUserID());
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void put(Request request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.put(request.getUserID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void withdraw(Request request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.withdraw(request.getUserID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void deposit(Request request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.deposit(request.getUserID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void transfer(TransferRequest request, StreamObserver<BooleanResponse> responseObserver) {
            boolean success = dbEngine.transfer(request.getFromID(), request.getToID(), request.getValue());
            BooleanResponse response = BooleanResponse.newBuilder().setSuccess(success).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void logLength(Null request, StreamObserver<GetResponse> responseObserver) {
            int value = dbEngine.getLogLength();
            GetResponse response = GetResponse.newBuilder().setValue(value).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}
