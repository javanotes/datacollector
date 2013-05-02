package test;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsmpp.InvalidResponseException;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.GeneralDataCoding;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.bean.SMSCDeliveryReceipt;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.util.AbsoluteTimeFormatter;
import org.jsmpp.util.TimeFormatter;

import com.logica.smpp.pdu.SubmitSM;

public class Test {

	static AtomicInteger ai = new AtomicInteger(0);
	static SubmitSM pdu = null;
	private static TimeFormatter timeFormatter = new AbsoluteTimeFormatter();;
	
	public static final int NO_OF_MSGS = 10;
	public static final int NO_OF_SESSIONS = 2;
	public static final int port = 2776;
	public static final String host = "localhost";
	
	static void multiSession(){
		SMPPSession [] sessions = new SMPPSession[NO_OF_SESSIONS];
		for(int i=0; i<NO_OF_SESSIONS; i++){
			SMPPSession session = new SMPPSession();
	        try {
	            session.connectAndBind(host, port, new BindParameter(BindType.BIND_TX, "test", "test", "cp", TypeOfNumber.UNKNOWN, NumberingPlanIndicator.UNKNOWN, null));
	            sessions[i]=session;
	        } catch (IOException e) {
	            System.err.println("Failed connect and bind to host");
	            e.printStackTrace();
	        }
		}
		
		
            for (int i = 0; i < NO_OF_MSGS; i++) {
            	
				try {
					String message = "Ths is a test text being sent to smsc " + i;
					String messageId = sessions[i % sessions.length].submitShortMessage("CMT",
							TypeOfNumber.INTERNATIONAL,
							NumberingPlanIndicator.UNKNOWN, "1616",
							TypeOfNumber.INTERNATIONAL,
							NumberingPlanIndicator.UNKNOWN, "628176504657",
							new ESMClass(), (byte) 0, (byte) 1,
							timeFormatter.format(new Date()), null,
							new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT),
							(byte) 0, new GeneralDataCoding(), (byte) 0,
							message.getBytes());
					
					System.out.println("Message submitted, message_id is "
							+ messageId);
					
				} catch (Throwable e) {
					
					e.printStackTrace();
				}
			}
            
            for(SMPPSession session : sessions){
            	session.unbindAndClose();
            }
        
	}
		
	static void singleSession(){
		SMPPSession session = new SMPPSession();
        try {
            session.connectAndBind("localhost", 8088, new BindParameter(BindType.BIND_TX, "test", "test", "cp", TypeOfNumber.UNKNOWN, NumberingPlanIndicator.UNKNOWN, null));
        } catch (IOException e) {
            System.err.println("Failed connect and bind to host");
            e.printStackTrace();
        }
        
        try {
            for (int i = 0; i < NO_OF_MSGS; i++) {
            	
				String messageId = session.submitShortMessage("CMT",
						TypeOfNumber.INTERNATIONAL,
						NumberingPlanIndicator.UNKNOWN, "1616",
						TypeOfNumber.INTERNATIONAL,
						NumberingPlanIndicator.UNKNOWN, "628176504657",
						new ESMClass(), (byte) 0, (byte) 1,
						timeFormatter.format(new Date()), null,
						new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT),
						(byte) 0, new GeneralDataCoding(), (byte) 0,
						"jSMPP simplify SMPP on Java platform".getBytes());
				
				System.out.println("Message submitted, message_id is "
						+ messageId);
			}
            
        } catch (org.jsmpp.PDUException e) {
            // Invalid PDU parameter
            System.err.println("Invalid PDU parameter");
            e.printStackTrace();
        } catch (ResponseTimeoutException e) {
            // Response timeout
            System.err.println("Response timeout");
            e.printStackTrace();
        } catch (InvalidResponseException e) {
            // Invalid response
            System.err.println("Receive invalid respose");
            e.printStackTrace();
        } catch (NegativeResponseException e) {
            // Receiving negative response (non-zero command_status)
            System.err.println("Receive negative response");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("IO error occur");
            e.printStackTrace();
        }
        
        session.unbindAndClose();

	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//singleSession();
		multiSession();
	}

}
