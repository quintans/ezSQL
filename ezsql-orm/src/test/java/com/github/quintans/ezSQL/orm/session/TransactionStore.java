package com.github.quintans.ezSQL.orm.session;

public class TransactionStore {

     private static final ThreadLocal < Transaction > store = 
         new ThreadLocal < Transaction > () {
             @Override protected Transaction initialValue() {
                 return new Transaction();
             }
     	};
 
     public static Transaction get() {
         return store.get();
     }
     
     public static void clear() {
    	 store.get().clear();
     }     

     public static boolean isInTransaction() {
         return store.get().isInTransaction();
     }     
     
     public static void hold() {
         store.get().setRunning(false);
     }     

     public static void resume() {
         store.get().setRunning(true);
     }     

     public static void enter() {
         store.get().incDepth();
     }     

     public static void exit() {
         store.get().decDepth();
     }     
}
