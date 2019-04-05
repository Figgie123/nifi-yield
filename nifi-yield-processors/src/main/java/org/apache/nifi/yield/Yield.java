/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.yield;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

// imports added by me after initial maven generate
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"yield","release"})
@CapabilityDescription("This processor will allow flowfiles to be yielded if there are other flowfiles processing before a Yield release occurs.")
@SeeAlso(classNames={"org.apache.nifi.yield.Release","org.apache.nifi.processors.standard.Wait","org.apache.nifi.processors.standard.Notify"})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class Yield extends AbstractProcessor {


    public static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("distributed-cache-service")
            .displayName("Distributed Cache Service")
            .description("The Controller Service that is used to check for yeild name from a corresponding Yield release processor")
            .required(true)
            .identifiesControllerService(AtomicDistributedMapCacheClient.class)
            .build();

    public static final PropertyDescriptor PROP_YIELD_SIGNAL = new PropertyDescriptor
            .Builder().name("yield-signal")
            .displayName("Yield Signal")
            .description("This is the cache key that is used to yield flow files.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

//    private static final String PROP_VALUE_YIELD = "yield";
//    private static final String PROP_VALUE_RELEASE = "release";
//    public static final PropertyDescriptor PROP_YIELD_TYPE = new PropertyDescriptor
//            .Builder().name("yield-type")
//            .displayName("Yield Type")
//            .description("The type of yield this processor is performing, either 'yield' or 'release'.")
//            .required(true)
//            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//            .allowableValues ( PROP_VALUE_YIELD, PROP_VALUE_RELEASE )
//            .build();

    public static final Relationship YIELD_RELATIONSHIP = new Relationship.Builder()
            .name("yield")
            .description("Where to send flowfiles if they need to be yielded.")
            .build();

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("success")
            .description("Where to send flowfiles if they can continue moving forward.")
            .build();

    public static final Relationship FAILURE_RELATIONSHIP = new Relationship.Builder()
            .name("failure")
            .description("Where to send flowfiles if an error occurs in this processor.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final Serializer<String> keySerializer = new StringSerializer();
    private final Deserializer<byte[]> valueDeserializer = new CacheValueDeserializer();
    private final Serializer<byte[]> valueSerializer = new CacheValueSerializer();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DISTRIBUTED_CACHE_SERVICE);
        descriptors.add(PROP_YIELD_SIGNAL);
//        descriptors.add(PROP_YIELD_TYPE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(YIELD_RELATIONSHIP);
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(FAILURE_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final AtomicDistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(AtomicDistributedMapCacheClient.class);
        //final AtomicCacheEntry<String, String, Object> entry = (AtomicCacheEntry<String, String, Object>) cache.fetch(signalId, stringSerializer, stringDeserializer);
        byte[] myCacheValue = null;
        try {
            myCacheValue = cache.get ( context.getProperty(PROP_YIELD_SIGNAL).getValue(), keySerializer, valueDeserializer );

            // TODO implement
//            String yieldType = context.getProperty ( PROP_YIELD_TYPE ).getValue();
//            if ( PROP_VALUE_YIELD.equals ( yieldType ) )
//            {
                if ( myCacheValue == null )
                {
                    // since we will go to success, then let's throw the signal up now
                    cache.put ( context.getProperty(PROP_YIELD_SIGNAL).getValue()
                              , context.getProperty(PROP_YIELD_SIGNAL).getValue().getBytes()
                              , keySerializer
                              , valueSerializer  );
                    // if there is no cache value, that means we can move forward
                    session.transfer ( flowFile, SUCCESS_RELATIONSHIP );
                }
                else
                {
                    // if there is a cache value, we need to yield
                    session.transfer ( flowFile, YIELD_RELATIONSHIP );
                }
//            }
//            else if ( PROP_VALUE_RELEASE.equals ( yieldType ) )
//            {
//                if ( myCacheValue == null )
//                {
//                    session.transfer ( flowFile, FAILURE_RELATIONSHIP );
//                }
//                else
//                {
//                    cache.remove ( context.getProperty(PROP_YIELD_SIGNAL).getValue()
//                                 , keySerializer );
//                    // if there is no cache value, that means we can move forward
//                    session.transfer ( flowFile, SUCCESS_RELATIONSHIP );
//                }
//            }
        } catch ( IOException e ) {
            session.transfer ( flowFile, FAILURE_RELATIONSHIP );
            throw new ProcessException(String.format("Unable to communicate with cache while updating %s due to %s", myCacheValue, e), e);
        }
    }

    /**
     * Simple string serializer, used for serializing the cache key
     *
     */
    public static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }

    public static class CacheValueSerializer implements Serializer<byte[]> {

        @Override
        public void serialize(final byte[] bytes, final OutputStream out) throws SerializationException, IOException {
            out.write(bytes);
        }
    }

    public static class CacheValueDeserializer implements Deserializer<byte[]> {

        @Override
        public byte[] deserialize(final byte[] input) throws DeserializationException, IOException {
            if (input == null || input.length == 0) {
                return null;
            }
            return input;
        }
    }

}
