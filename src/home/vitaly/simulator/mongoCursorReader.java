package home.vitaly.simulator;

import com.entrib.mongo2gson.Mongo2gson;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.*;
import home.vitaly.datamodel.Transaction;

/**
 * Created with IntelliJ IDEA.
 * User: vitaly
 * Date: 07.11.13
 * Time: 17:20
 */
public class mongoCursorReader  {

        private Mongo getConnection() throws Exception {
            return new Mongo("localhost:27017");
        }

        public DBCollection getCollection(String collectionName, DB db) {
            return db.getCollection(collectionName);
        }

        public static void main(String[] args) throws Exception {
            Mongo conn = null;

            mongoCursorReader manager = new mongoCursorReader();
            try {
                // Get the MongoDB connection.  Mongo is the connection object.
                conn = manager.getConnection();

                // Get the database from the Connection (Schema in RDBMS World).
                DB db = conn.getDB("DBTR");

                // Get the collection (Table in RDBMS World).  Don't worry, even if it is not there,
                // it can be a lazy fetch and will be created when the first object is inserted
                DBCollection collection = manager.getCollection("tr", db);

                // Check whether there is any document (RDBMS world -> Row), present
//            if (collection.count() == 0) {
//            if(collection.count() < 10) {
//                // Create the document in the TEST collection (RDBMS worlqd -> Table)
//                DBObject object = new BasicDBObject();
//                object.put("name", "Нечто такое");
//                object.put("message", "Здорово усатый !");
//                collection.insert(object);
//            }
                manager.printCollection(collection);
            } finally {
                if (conn != null) {
                    conn.close();
                }
            }
        }

        public void printCollection(DBCollection collection) {
            // Get the cursor (ResultSet in RDBMS World)
            DBCursor cursor = collection.find();
            Mongo2gson m2g = new Mongo2gson();
            Gson gsonBuilder = new GsonBuilder().setPrettyPrinting().create();
            int i=0;
            while (cursor.hasNext()) {
                i++;
                DBObject o = cursor.next();
                com.google.gson.JsonObject jsonObject = m2g.getAsJsonObject(o);
                gsonBuilder.fromJson(jsonObject, Transaction.class);
        }
    }
