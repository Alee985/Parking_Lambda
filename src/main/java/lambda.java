import com.google.gson.Gson;

import java.util.Date;

public class lambda {
    public static void main(String[] args) {

        while(true) {
            if (SQS.getCount() > 0) {
                String msg = SQS.ReadMessage();

                ParkInfo info = new Gson().fromJson(msg, ParkInfo.class);
                String emailAddr = "";
                String emailMsg = "";

                ParkInfo p = new ParkInfo();
                if (info.getStatus().equals("in")) {


                    Dynamo.createTable();
                    Dynamo.insertData(info);
                    emailAddr = info.getEmail();
                    System.out.println("Inserted in Table.....");
                    emailMsg = "You have been Parked in at ParK_Key at " + info.getParkin();
                } else {


                    emailAddr = Dynamo.updateData(info.getV_Name(), info.getParkout());
                    emailMsg = "You have been Parked out of ParK_Key at " + info.getParkout();

                }

                Email.sendEmail(emailAddr, emailMsg);
                System.out.println("Email has been Sent.....");

            }
            else
                System.out.println("Queue is Empty....");
        }
    }
}
