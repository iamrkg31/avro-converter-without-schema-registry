# Avro Converter Without Schema Registry

This is an Avro converter for Kafka Connect that does not depend on Confluent Schema Registry. It shares much of the
same underlying code as Confluent's `AvroConverter`, and should work the same in practice less any features that deal
with the Schema Registry itself.

## Using the Converter

### Setup

1. You must have **Java 8** as your runtime environment.
2. **Confluent Platform**: as this plugin relies on various Confluent libraries that are distributed with CP (e.g. their
   avro converter, etc). See the chart below for the version matrix.
3. Configure a `plugin.path` in your connect setup and **drop the built jar (from `build/libs` directory) in the directory having
   confluent jars for Avro Converter**, so that its picked up with Kafka Connect.

Once you've confirmed that the binary is in place, then in a properties file or JSON connector configuration you can
specify this converter for keys and/or values.

### Configuration

To use the converter, simply provide following configs for your connector.

```
key.converter=me.frmr.kafka.connect.RegistrylessAvroConverter
key.converter.schema.path=/path/to/schema/file.avsc
key.converter.binaryencoding.enable=true
value.converter=me.frmr.kafka.connect.RegistrylessAvroConverter
value.converter.schema.path=/path/to/schema/file.avsc
value.converter.binaryencoding.enable=true
```

By default `key.converter.binaryencoding.enable` setting is `true`, so we use binary encoding to serialize/deserialize
the avro message if no value provided for this setting. If it's other than true, then we expect the plain json string as
the avro message.

You can also tune the number of cached schemas we maintain in memory. By default, we store 50 but you may need to
increase that limit if your data structures have a lot of nesting or you're dealing with a lot of different data
structure. You can tune it using the `schema.cache.size` setting:

```
key.converter.schema.cache.size = 100
value.converter.schema.cache.size = 100
```

Unfortunately, the best way to _know_ you need to tune this value right now might be to hook up YourKit or something
similar.

## Building the Converter

This converter uses Gradle. Building the project is as simple as:
```
./gradlew build
```

## Producers to test the converter
* Using binary encoding for serialization/deserialization - 
  [link](https://github.com/iamrkg31/kafka/tree/main/kafka-consumer-producer)
* Using plain JSON string (schema - [link](https://github.com/iamrkg31/avro-converter-without-schema-registry))
```
bin/kafka-console-producer.sh --topic test_topic --bootstrap-server localhost:9092
> {"name": "Tony","id": "Iron Man","age": 50,"birthDate": 1621016565}
```


## General Notes

* This project is a bit weird because it's designed to be run in a Kafka Connect runtime. So all of the dependencies
  are `compileOnly` because they're available on the classpath at runtime.
* If you're testing this locally, it's a bit weird in much the same way. You'll need to copy the JAR into an appropriate
  plugin path folder (as configured on your Connect worker) so the class is visible to Kafka Connect for local testing.

## Contributing
This project is forked from github repository - [RegistrylessAvroConverter](https://github.com/farmdawgnation/registryless-avro-converter). All the credit goes to him for this amazing work. I have only tweaked a few things to meet my requirements.
<br/><br/>
Pull requests and issues are welcome! If you think you've spotted a problem or you just have a question do not hesitate
to [open an issue](https://github.com/farmdawgnation/registryless-avro-converter/issues/new).
