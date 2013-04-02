package com.egi.datacollector.processor.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.egi.datacollector.processor.Data;

public class FileData implements Data {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2835493968346669382L;
	private final List<String> fileNames = Collections.synchronizedList(new ArrayList<String>());
	
	public void addFile(String file){
		fileNames.add(file);
	}
	
	private boolean memMapIO = false;
	public List<String> getFiles(){
		return fileNames;
		
	}

	@Override
	public String type() {
		return "file";
	}

	public boolean isMemMapIO() {
		return memMapIO;
	}

	public void setMemMapIO(boolean memMapIO) {
		this.memMapIO = memMapIO;
	}

}
