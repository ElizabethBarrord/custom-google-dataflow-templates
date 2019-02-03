package com.iot.fitness.gcp;
import java.io.File;
import java.util.List;
import java.util.Map;
 
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
 
public class csv2Json {
 
    public static void main(String[] args) throws Exception {
        File input = new File("../../date_right_csv.csv");
        File output = new File("output.json");
 
        CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
        CsvMapper csvMapper = new CsvMapper();
 
        // Read data from CSV file
        List<Object> readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(input).readAll();
 
        ObjectMapper mapper = new ObjectMapper();
        
        mapper.writerWithDefaultPrettyPrinter().writeValue(output, readAll);

        String jsonString = "";
        for (Object obj : readAll) {
        	jsonString += mapper.writeValueAsString(obj);
        	jsonString += "\n";
        }
        
        System.out.println(jsonString);
    }
}