import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class jpianoPush {
   
    private static String qUrl = ""; //queue&exit
    private static String sUrl = ""; //ending
    private static String auth = ""; //auth token


    public static Timestamp showTime(String data) throws ParseException {
        DateFormat df = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        Date daTa = df.parse(data);
        long time = daTa.getTime();

        return new Timestamp(time);
    }

    public static void sendToDb(HashMap<String,Object> dict) throws IOException, InterruptedException {
        String url="";
        JSONObject json = new JSONObject(dict);
        //System.out.println(json.toString());

        if(dict.containsValue("pull") || dict.containsValue("push")){
            url=qUrl;
        }else{
            url=sUrl;
        }

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("content-type","application/json")
                .header("authorization",auth)
                .POST(HttpRequest.BodyPublishers.ofString(json.toString()))
                .build();

        HttpResponse<String> res = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(res);
    }

    public static void push(String date, String name, int size) throws ParseException, IOException, InterruptedException {
        HashMap<String, Object> pushJ = new HashMap<>();
        HashMap<String,Object> nested = new HashMap<>();
        Timestamp ts = showTime(date);

        nested.put("size",size); nested.put("service",name);
        pushJ.put("result","push"); pushJ.put("data",nested);
        pushJ.put("name","service"); pushJ.put("source","francesco_test"); pushJ.put("timestamp",ts.getTime());

        sendToDb(pushJ);

        /*for(Object s : pushJ.values()){
            System.out.print(s.toString()+"\n");
        }*/
    }

    public static void pull(String date, String name) throws ParseException, IOException, InterruptedException {
        HashMap<String,Object> pullJ = new HashMap<>();
        HashMap<String,Object> nestedP = new HashMap<>();
        Timestamp ts = showTime(date);

        nestedP.put("service",name); pullJ.put("data",nestedP);
        pullJ.put("result","pull"); pullJ.put("name","queue"); pullJ.put("source","francesco_test");
        pullJ.put("timestamp",ts.getTime());

        /*for(Object s : pullJ.values()){
            System.out.print(s.toString()+"\n");
        }*/

        sendToDb(pullJ);


    }

    public static void end(String date, String name,Integer processTime, Integer queueTime,String result) throws ParseException, IOException, InterruptedException {

        HashMap<String,Object> ending = new HashMap<>();
        HashMap<String,Object> nestedE = new HashMap<>();

        Timestamp ts = showTime(date);

        nestedE.put("service",name); nestedE.put("processtime",processTime); nestedE.put("queuetime",queueTime);
        ending.put("result",result); ending.put("data",nestedE); ending.put("name",name);
        ending.put("source","francesco_test"); ending.put("timestamp",ts.getTime());

        sendToDb(ending);

        //System.out.println(processTime+" "+queueTime);

       /* for(Object s : ending.values()){
            System.out.print(s.toString()+"\n");
        }*/

    }

    public static void events(String line) throws ParseException, IOException, InterruptedException {
        String queue = "^(\\d{2}\\/\\d{2}\\/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d)\\s\\[.*?\\]\\[Enqueued service (\\w+) for processing \\(size=(\\d+)\\).*\\]$";
        String exit = "^(\\d{2}\\/\\d{2}\\/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d)\\s\\[.*?\\]\\[<-- REQUEST NAME: (\\w+) - PARAMS:.*\\]$";
        String end = "^(\\d{2}/\\d{2}/\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d)\\s(?:ServiceProcessor:\\s\\[.*?]\\s|\\[.*]\\[)-->\\sANSWER\\sTO:\\s(\\w+)\\sTIME\\(ms\\)(?:\\s\\[process\\s-\\selapsed]:\\s(\\d+)\\s-\\s(\\d+)|:\\s(\\d+))\\sRESULT:\\s(?:(\\w+)(.*$|.*]$))";

        Pattern pQueue = Pattern.compile(queue);
        Pattern pExit = Pattern.compile(exit);
        Pattern pEnd = Pattern.compile(end);

        Matcher mQueue = pQueue.matcher(line);
        Matcher mExit = pExit.matcher(line);
        Matcher mEnd = pEnd.matcher(line);


        if(mEnd.matches()){
            Integer processtime=null, queuetime=null;
            if(mEnd.group(3)==null){
                queuetime=Integer.parseInt(mEnd.group(5));
            }

            if(mEnd.group(5)==null){
                processtime=Integer.parseInt(mEnd.group(3));
                queuetime=Integer.parseInt(mEnd.group(4));
            }

            end(mEnd.group(1),mEnd.group(2),processtime,queuetime,mEnd.group(6));

        }

        if(mExit.matches()) {
            pull(mExit.group(1), mExit.group(2));
        }

        if(mQueue.matches()) {
           push(mQueue.group(1),mQueue.group(2),Integer.parseInt(mQueue.group(3)));
        }

    }

    public static void main(String args[]) throws IOException, ParseException, InterruptedException {
        BufferedReader follow = new BufferedReader(new FileReader("follow.txt"));
        String currentLine = null;

        while (true) {
            if ((currentLine = follow.readLine()) != null) {
                events(currentLine);
                continue;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
    }


    }

}
