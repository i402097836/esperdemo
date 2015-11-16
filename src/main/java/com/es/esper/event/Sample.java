package com.es.esper.event;

import com.espertech.esper.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by MQ. EL on 2015/11/11.
 */

/**
 * 1.event流中属性自身包含句点".", 例如本例中属性table1.L1 是一个属性，而不是table1中的L1属性，
 * 此时需要用转义字符“\”  例如：table1\.L1  转义后就表示一个属性。
 * 2.当EPL语句中有系统预定的关键字时，使用the backwards apostrophe(后撇号“`”，键盘中数字1左侧的键)。
 * 3.给属性起别名同样使用后撇号"`"。
 * 4.对于取map中的某一属性值,语法为:mapname('key')
 */
public class Sample {

    public static void main(String[] args) throws InterruptedException {

        int numEvents = 10;
        int numThreads = 1;
        Configuration config = new Configuration();
        config.getEngineDefaults().getThreading()
                .setListenerDispatchPreserveOrder(false);
        config.getEngineDefaults().getThreading()
                .setInternalTimerEnabled(false);   // remove thread that handles time advancing
        EPServiceProvider engine = EPServiceProviderManager
                .getDefaultProvider(config);
        engine.getEPAdministrator().getConfiguration().addEventType(MyEvent.class);

//        engine.getEPAdministrator().createEPL(
//                "create context MyContext coalesce by consistent_hash_crc32(id) " +
//                        "from MyEvent granularity 64 preallocate");

        final Map<String, Object> L1 = new HashMap<String, Object>();
        final Map<String, Object> L2 = new HashMap<String, Object>();
        final Map<String, Object> L3 = new HashMap<String, Object>();

        L3.put("L3", int.class);
        L2.put("L2", L3);
        L1.put("table1.L1", Boolean.class);
        L1.put("L1", L3);
        L1.put("order",String.class);
        L1.put("subL1",Map.class);

        engine.getEPAdministrator().getConfiguration().addEventType("table1", L1);
//      String epl = "context MyContext select * from MyEvent group by id";
        String epl = "select table1.L1 ,`order` as `orders`,subL1('age') from table1";
        EPStatement stmt = engine.getEPAdministrator().createEPL(epl);
        stmt.setSubscriber(new MySubscriber());

        Thread[] threads = new Thread[numThreads];
        CountDownLatch latch = new CountDownLatch(numThreads);

        int eventsPerThreads = numEvents / numThreads;
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(
                    new MyRunnable(latch, eventsPerThreads, engine.getEPRuntime()));
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }

        latch.await(10, TimeUnit.MINUTES);
        if (latch.getCount() > 0) {
            throw new RuntimeException("Failed to complete in 10 minute");
        }
        long delta = System.currentTimeMillis() - startTime;
        System.out.println("Took " + delta + " millis");
    }

    public static class MySubscriber {
        public void update(Map args) {
            System.out.println(args);
//            System.out.println(Thread.currentThread());
        }
    }

    public static class MyRunnable implements Runnable {
        private final CountDownLatch latch;
        private final int numEvents;
        private final EPRuntime runtime;
        
        private final Map<String, Object> L1 = new HashMap<String, Object>();
        private final Map<String, Object> L2 = new HashMap<String, Object>();
        private final Map<String, Object> L3 = new HashMap<String, Object>();
        
        
        

        public MyRunnable(CountDownLatch latch, int numEvents, EPRuntime runtime) {
            this.latch = latch;
            this.numEvents = numEvents;
            this.runtime = runtime;
            
            L3.put("L3", 6);
            L2.put("L2", L3);
            L1.put("table1.L1", true);
            L1.put("L1", L3);
            L1.put("order","a reserved keyword");
            Map subL1= new HashMap();
            subL1.put("name","eastsoft");
            subL1.put("age",110);
            L1.put("subL1",subL1);
        }

        public void run() {
            Random r = new Random();
            for (int i = 0; i < numEvents; i++) {
//                runtime.sendEvent(new MyEvent(r.nextInt(512)));
                runtime.sendEvent(L1, "table1");
            }
            latch.countDown();
        }
    }

    public static class MyEvent {
        private final int id;
        
        private final DD dd = new DD(); 

        public MyEvent(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }
        
        public DD getDd() { return dd;}
    }
    
    public static class DD {
        int i = 6;
    }
}
