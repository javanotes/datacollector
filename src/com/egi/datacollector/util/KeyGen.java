package com.egi.datacollector.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.List;

public class KeyGen {
	
	static String cipherAlgorithm = "DSA";
	static String outFilePath = System.getProperty("user.dir");
	
	private static void generate(){
						
		FileOutputStream fPub = null;
		FileOutputStream fPri = null;
		try {
			KeyPairGenerator gen = KeyPairGenerator.getInstance(cipherAlgorithm);
			gen.initialize(1024);
			KeyPair pair = gen.generateKeyPair();
			
			PublicKey generatedPublicKey = pair.getPublic();
			PrivateKey generatedPrivateKey = pair.getPrivate();
			boolean saved = false;
			try {
				fPub = new FileOutputStream(outFilePath + File.separator + "public.key");
				fPub.write(generatedPublicKey.getEncoded());
				fPub.flush();
				
				fPri = new FileOutputStream(outFilePath + File.separator + "private.key");
				fPri.write(generatedPrivateKey.getEncoded());
				fPri.flush();
				saved = true;
				System.out.println("Key pair generated for algorithm ["+cipherAlgorithm+"]. Use the same algorithm while performing signature validations.");
				System.out.println("Public key: " + outFilePath + File.separator + "public.key");
				System.out.println("Private key: " + outFilePath + File.separator + "private.key");
								
			} catch (FileNotFoundException e) {
				System.err.println("Unable to generate key files ERROR: " + e.getMessage());
				System.out.println("Usage: java KeyGen -p <path_to_generated_files(optional)>");
			} catch (IOException e) {
				System.err.println("Unable to generate key files ERROR: " + e.getMessage());
				System.out.println("Usage: java KeyGen -p <path_to_generated_files(optional)>");
			}
			finally{
				if(!saved){
					
					File f = new File(outFilePath + File.separator + "public.key");
					if(f.exists() && f.isFile())
						f.delete();
					
					f = new File(outFilePath + File.separator + "private.key");
					if(f.exists() && f.isFile())
						f.delete();
					
				}
				if (fPub != null) {
					try {
						fPub.close();
					} catch (IOException e) {

					}
				}
				if (fPri != null) {
					try {
						fPri.close();
					} catch (IOException e) {

					}
				}
			}
			
		} catch (NoSuchAlgorithmException e) {
			System.err.println("Unable to generate key files ERROR: " + e.getMessage());
			System.out.println("Usage: java KeyGen -p <path_to_generated_files(optional)>");
		} 
		
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		System.out.println("Starting key generation ...");
			
		if(args.length > 0){
			List<String> params = Arrays.asList(args);
			int pos = -1;
			/*if((pos = params.indexOf("a")) != -1){
				cipherAlgorithm = params.get(pos + 1);
			}*/
			if((pos = params.indexOf("p")) != -1){
				outFilePath = params.get(pos + 1);
			}
		}
		generate();
		
	}

}
