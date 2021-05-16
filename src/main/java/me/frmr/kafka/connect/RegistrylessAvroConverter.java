/**
 * Copyright 2018 Matt Farmer (github.com/farmdawgnation)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.frmr.kafka.connect;

import io.confluent.connect.avro.AvroData;
import java.io.File;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.apache.avro.SchemaParseException;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Converter that uses Avro schemas and objects without using
 * an external schema registry. Requires that a `schema.path` configuration
 * option is provided that tells the converter where to find its Avro schema.
 */
public class RegistrylessAvroConverter implements Converter {
	private static Logger logger = LoggerFactory.getLogger(RegistrylessAvroConverter.class);

	/**
	 * The default schema cache size. We pick 50 so that there's room in the cache
	 * for some recurring nested types in a complex schema.
	 */
	private Integer schemaCacheSize = 50;

	private org.apache.avro.Schema avroSchema = null;
	private AvroData avroDataHelper = null;
	private boolean binaryEncoding = true; 

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (configs.get("schema.cache.size") instanceof Integer) {
			schemaCacheSize = (Integer) configs.get("schema.cache.size");
		}
		avroDataHelper = new AvroData(schemaCacheSize);

		logger.info("----------------------->"+configs.get("binaryencoding.enable").getClass());
		if (configs.get("binaryencoding.enable") instanceof String) {
			binaryEncoding = Boolean.parseBoolean((String) configs.get("binaryencoding.enable"));
			logger.info("----------------------->"+(String) configs.get("binaryencoding.enable"));
			logger.info("----------------------->"+binaryEncoding);
		}
		
		if (configs.get("schema.path") instanceof String) {
			String avroSchemaPath = (String) configs.get("schema.path");
			org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

			File avroSchemaFile = null;
			try {
				avroSchemaFile = new File(avroSchemaPath);
				avroSchema = parser.parse(avroSchemaFile);
			} catch (SchemaParseException spe) {
				throw new IllegalStateException("Unable to parse Avro schema when starting RegistrylessAvroConverter",
						spe);
			} catch (IOException ioe) {
				throw new IllegalStateException("Unable to parse Avro schema when starting RegistrylessAvroConverter",
						ioe);
			}
		}
	}

	@Override
	public byte[] fromConnectData(String topic, Schema schema, Object value) {
		byte[] bytes = null;
		try {			
			GenericRecord avroInstance = (GenericRecord) avroDataHelper.fromConnectData(schema, value);
			if (binaryEncoding) {
				DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
				ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
				BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);				
				datumWriter.write(avroInstance, binaryEncoder);
				binaryEncoder.flush();
				byteArrayOutputStream.close();
				bytes = byteArrayOutputStream.toByteArray();
			}else {
				bytes = avroInstance.toString().getBytes();
			}				
			return bytes;
		} catch (IOException ioe) {
			throw new DataException("Error serializing avro", ioe);
		}
	}

	@Override
	public SchemaAndValue toConnectData(String topic, byte[] value) {
		try {		    
			DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(avroSchema);
			Object instance = null;
			if (binaryEncoding) {
				Decoder decoder = DecoderFactory.get().binaryDecoder(value, null);
				instance = datumReader.read(null, decoder);
			}else {
				Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, new ByteArrayInputStream(value));
				instance = datumReader.read(null, decoder);
			}
			return avroDataHelper.toConnectData(avroSchema, (GenericRecord) instance);
		} catch (IOException ioe) {
			throw new DataException("Failed to deserialize avro data", ioe);
		}
	}
}
