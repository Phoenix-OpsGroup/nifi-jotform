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

import org.apache.commons.io.FileUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertTrue;


public class JotformReformatJsonProcessorTest
{

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(JotformReformatJsonProcessor.class);
    }

    @Test
    @Ignore
    public void testProcessor() {

        try
        {
            // Mock content
            File fSubmission = new File("../test-data/jotform-lib/jotform.outreach.submission.json");
            InputStream content = FileUtils.openInputStream(fSubmission);

            File fFormSchema = new File("../test-data/jotform-lib/jotform.outreach.form.questions.json");
            String formSchemaJson = FileUtils.readFileToString(fFormSchema, StandardCharsets.UTF_8);
            // TODO How to get this as a Flowfile Attribute prior to testing

            // Mock Processor
            TestRunner runner = TestRunners.newTestRunner(new JotformReformatJsonProcessor());

            // Add Properties
//            runner.setProperty("key","value");
//            runner.setProperty("form.schema",formSchemaJson);
            
            // Add content to Mock Processor
            runner.enqueue(content);

            // Run the content through
            runner.run(1);

            // All results were processed with out failure
            runner.assertQueueEmpty();

            // If you need to read or do additional tests on results you can access the content
            List<MockFlowFile> results = runner.getFlowFilesForRelationship(JotformReformatJsonProcessor.SUCCESS);
            assertTrue("1 match", results.size() == 1);
            MockFlowFile result = results.get(0);
            String resultValue = new String(runner.getContentAsByteArray(result));
            
            System.out.println("Match: " + new String(runner.getContentAsByteArray(result)));

            // Test attributes and content
//            result.assertAttributeEquals(JotformReformatJsonProcessor.MATCH_ATTR, "nifi rocks");
//            result.assertContentEquals("nifi rocks");

        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
