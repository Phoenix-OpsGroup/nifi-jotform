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
package com.phoenixopsgroup.nifi.jotform;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.phoenixopsgroup.jotform.gson.*;
import com.phoenixopsgroup.jotform.util.JotformResponse;
import com.phoenixopsgroup.jotform.util.JotformUtil;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"Jotform", "JSON", "Elasticsearch"})
@CapabilityDescription("Reformat Jotform JSON suitable for Elasticsearch")
//@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
//@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class JotformReformatJsonProcessor extends AbstractProcessor {
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    public static final Relationship FAIL = new Relationship.Builder()
            .name("FAIL")
            .description("Fail relationship")
            .build();
    
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAIL);
        
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session)
            throws ProcessException {

        final ComponentLog log = this.getLogger();

        final AtomicReference<String> value = new AtomicReference<>();
        final AtomicReference<String> formSchema = new AtomicReference<>();

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        formSchema.set(flowFile.getAttribute("jotform.form.schema"));

        // We expect to receive a Jotform Submission JSON formatted response
        // curl -X GET "https://api.jotform.com/submission/{SUBMISSION_ID}?apiKey={API+KEY}"
        
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException
            {
                try{
                    String json = IOUtils.toString(in);

                    final GsonBuilder gsonBuilder = new GsonBuilder();
                    gsonBuilder.registerTypeAdapter(Answer.class, new AnswerTypeAdapter());
                    final Gson gson = gsonBuilder.create();

                    JotformSubmission jotformSubmission
                            = gson.fromJson(json, JotformSubmission.class);

                    final GsonBuilder gsonBuilder2 = new GsonBuilder();
                    gsonBuilder2.registerTypeAdapter(FormQuestion.class, new FormQuestionContentTypeAdapter());
                    final Gson gson2 = gsonBuilder2.create();

                    FormQuestionResponse formQuestionResponse
                            = gson2.fromJson(formSchema.get(), FormQuestionResponse.class);

                    JotformUtil jotformUtil = new JotformUtil();
                    JotformResponse jotformResponse = jotformUtil.getResponse(jotformSubmission, formQuestionResponse);

                    value.set(jotformResponse.toJson(false));

                }catch(Exception ex){
                    ex.printStackTrace();
                    log.error("Failed to read json string.");
                }
            }
        });

        
        flowFile = session.write(flowFile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        session.transfer(flowFile, SUCCESS);
        
    }
}
