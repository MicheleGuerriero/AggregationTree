package it.aggregationtree.utils;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.aggregationtree.Row;

public class InputLoader<V> {

	/**
	 * Loads a dataset or rows from an external JSON file.
	 * 
	 * @param jsonInput: the JSON file to be loaded.
	 */
	public List<Row<V>> loadFromJson(File jsonInput){
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(jsonInput, new TypeReference<List<Row<V>>>(){});
			
		} catch (JsonParseException e) {
			throw new RuntimeException("Error parsing the input json file.");
		} catch (JsonMappingException e) {
			throw new RuntimeException("Error mapping the input json file to  objects.");
		} catch (IOException e) {
			throw new RuntimeException("Problem loading the input json file. Make sure it exists at the provided  path.");
		}
	}

}
