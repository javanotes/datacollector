package com.egi.datacollector.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import org.apache.commons.codec.binary.Base64;

public class Crypto {
	
	private static String cipherAlgorithm = "DSA";	
	/**
	 * Method to generate a digital signature
	 * @param signatureText The signature clear text
	 * @param privateKeyFile Path to the private key
	 * @return Base64 encoded signed bytes
	 * @throws Exception 
	 */
	public static String generateSignature(String signatureText, String privateKeyFile) throws Exception{
		FileInputStream fis = null;
		PKCS8EncodedKeySpec encodedPrivateKey = null;
		try {
			
			fis = new FileInputStream(privateKeyFile);
			byte [] bytes = new byte [fis.available()];
			fis.read(bytes);
			try {
				fis.close();
			} catch (IOException e) {
				
			}
			encodedPrivateKey = new PKCS8EncodedKeySpec(bytes);
			
			Signature sign = Signature.getInstance(cipherAlgorithm);
			PrivateKey _private = KeyFactory.getInstance(cipherAlgorithm).generatePrivate(encodedPrivateKey);
			sign.initSign(_private);
			sign.update(signatureText.getBytes());
			byte [] signed = sign.sign();
			
			return Base64.encodeBase64String(signed);
			
		}  catch (Exception e) {
			
			throw e;
		}
				
	}
	
	/**
	 * Method to verify a digital signature
	 * @param signatureText The signature clear text
	 * @param encodedSignatureText Base64 encoded signed bytes
	 * @param publicKeyFile Path to the public key
	 * @return boolean
	 * @throws Exception 
	 */
	public static boolean verifySignature(String signatureText, String encodedSignatureText, String publicKeyFile) throws Exception{
		FileInputStream fis = null;
		X509EncodedKeySpec encodedPublicKey = null;
		try {
			fis = new FileInputStream(publicKeyFile);
			byte [] bytes = new byte [fis.available()];
			fis.read(bytes);
			try {
				fis.close();
			} catch (IOException e) {
				
			}
			encodedPublicKey = new X509EncodedKeySpec(bytes);
			Signature sign = Signature.getInstance(cipherAlgorithm);
			PublicKey _public = KeyFactory.getInstance(cipherAlgorithm).generatePublic(encodedPublicKey);
			sign.initVerify(_public);
			sign.update(signatureText.getBytes());
			return sign.verify(Base64.decodeBase64(encodedSignatureText));
			
		} catch (Exception e) {
			
			throw e;
		} 
		
		
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String signature = generateSignature("ESUTDAL", "/home/esutdal/workspaces/github/datacollector/private.key");
			System.out.println("digital signature: " + signature);
			boolean verified = verifySignature("ESUTDAL", signature, "/home/esutdal/workspaces/github/datacollector/public.key");
			System.out.println("Authenticated: " + verified);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
