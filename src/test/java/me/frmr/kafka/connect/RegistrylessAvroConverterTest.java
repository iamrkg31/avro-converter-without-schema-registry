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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.connect.data.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class RegistrylessAvroConverterTest {
	@Test
	void configureWorksOnParsableSchema() {
		String validSchemaPath = new File("src/test/resources/schema/person.avsc").getAbsolutePath();

		RegistrylessAvroConverter sut = new RegistrylessAvroConverter();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("schema.path", validSchemaPath);

		sut.configure(settings, false);
	}

	@Test
	void configureThrowsOnInvalidSchema() {
		String invalidSchemaPath = new File("src/test/resources/schema/invalid.avsc").getAbsolutePath();

		RegistrylessAvroConverter sut = new RegistrylessAvroConverter();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("schema.path", invalidSchemaPath);

		Throwable resultingException = assertThrows(IllegalStateException.class, () -> sut.configure(settings, false));
		assertEquals("Unable to parse Avro schema when starting RegistrylessAvroConverter",
				resultingException.getMessage());
	}

	@Test
	void fromConnectDataWorksWithBinaryEncoding() throws Exception {
		File validSchema = new File("src/test/resources/schema/person.avsc");

		RegistrylessAvroConverter sut = new RegistrylessAvroConverter();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("schema.path", validSchema.getAbsolutePath());
		sut.configure(settings, false);

		Schema schema = SchemaBuilder.struct().name("Person").field("name", Schema.STRING_SCHEMA)
				.field("id", Schema.STRING_SCHEMA).field("age", Schema.INT32_SCHEMA)
				.field("birthDate", Schema.INT64_SCHEMA).build();

		Struct struct = new Struct(schema).put("name", "Tony").put("id", "Iron Man").put("age", 50).put("birthDate",
				(long) 1621016);

		byte[] result = sut.fromConnectData("test_topic", schema, struct);

		DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(
				new org.apache.avro.Schema.Parser().parse(validSchema));
		GenericRecord instance = null;
		try {
			Decoder decoder = DecoderFactory.get().binaryDecoder(result, null);
			instance = datumReader.read(null, decoder);
			assertEquals("Tony", instance.get("name").toString());
			assertEquals("Iron Man", instance.get("id").toString());
			assertEquals(50, instance.get("age"));
			assertEquals((long) 1621016, instance.get("birthDate"));
		} catch (IOException ioe) {
			throw new Exception("Failed to deserialize Avro data", ioe);
		}
	}

	@Test
	void fromConnectDataWorksWithoutBinaryEncoding() throws Exception {
		File validSchema = new File("src/test/resources/schema/person.avsc");

		RegistrylessAvroConverter sut = new RegistrylessAvroConverter();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("schema.path", validSchema.getAbsolutePath());
		settings.put("binaryencoding.enable", "false");
		sut.configure(settings, false);

		Schema schema = SchemaBuilder.struct().name("Person").field("name", Schema.STRING_SCHEMA)
				.field("id", Schema.STRING_SCHEMA).field("age", Schema.INT32_SCHEMA)
				.field("birthDate", Schema.INT64_SCHEMA).build();

		Struct struct = new Struct(schema).put("name", "Tony").put("id", "Iron Man").put("age", 50).put("birthDate",
				(long) 1621016);

		byte[] result = sut.fromConnectData("test_topic", schema, struct);
		System.out.println(new String(result));
		DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(
				new org.apache.avro.Schema.Parser().parse(validSchema));
		GenericRecord instance = null;
		try {
			Decoder decoder = DecoderFactory.get().jsonDecoder(new org.apache.avro.Schema.Parser().parse(validSchema), new ByteArrayInputStream(result));
			instance = datumReader.read(null, decoder);
			assertEquals("Tony", instance.get("name").toString());
			assertEquals("Iron Man", instance.get("id").toString());
			assertEquals(50, instance.get("age"));
			assertEquals((long) 1621016, instance.get("birthDate"));
		} catch (IOException ioe) {
			throw new Exception("Failed to deserialize Avro data", ioe);
		}
	}
	
	@Test
	void toConnectDataWithBinaryEncoding() throws IOException {
		InputStream in = this.getClass().getClassLoader().getResourceAsStream("data/binary/person.avro");
		byte[] data = IOUtils.toByteArray(in);

		String validSchemaPath = new File("src/test/resources/schema/person.avsc").getAbsolutePath();

		RegistrylessAvroConverter sut = new RegistrylessAvroConverter();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("schema.path", validSchemaPath);
		sut.configure(settings, false);

		SchemaAndValue sav = sut.toConnectData("test_topic", data);

		Schema schema = sav.schema();
		assertEquals(Schema.Type.STRING, schema.field("name").schema().type());
		assertEquals(Schema.Type.STRING, schema.field("id").schema().type());
		assertEquals(Schema.Type.INT32, schema.field("age").schema().type());
		assertEquals(Schema.Type.INT64, schema.field("birthDate").schema().type());

		Struct struct = (Struct) sav.value();
		assertEquals("Tony", struct.getString("name"));
		assertEquals("Iron Man", struct.getString("id"));
		assertEquals(50, struct.getInt32("age"));
		assertEquals(1621016, struct.getInt64("birthDate"));
	}

	@Test
	void toConnectDataWithoutBinaryEncoding() throws IOException {
		InputStream in = this.getClass().getClassLoader().getResourceAsStream("data/json/person.json");
		byte[] data = IOUtils.toByteArray(in);

		String validSchemaPath = new File("src/test/resources/schema/person.avsc").getAbsolutePath();

		RegistrylessAvroConverter sut = new RegistrylessAvroConverter();
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("schema.path", validSchemaPath);
		settings.put("binaryencoding.enable", "false");
		sut.configure(settings, false);

		SchemaAndValue sav = sut.toConnectData("test_topic", data);

		Schema schema = sav.schema();
		assertEquals(Schema.Type.STRING, schema.field("name").schema().type());
		assertEquals(Schema.Type.STRING, schema.field("id").schema().type());
		assertEquals(Schema.Type.INT32, schema.field("age").schema().type());
		assertEquals(Schema.Type.INT64, schema.field("birthDate").schema().type());

		Struct struct = (Struct) sav.value();
		assertEquals("Tony", struct.getString("name"));
		assertEquals("Iron Man", struct.getString("id"));
		assertEquals(50, struct.getInt32("age"));
		assertEquals(1621016, struct.getInt64("birthDate"));
	}

}
