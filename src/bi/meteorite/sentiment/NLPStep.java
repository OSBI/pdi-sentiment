/*
* Copyright 2014 OSBI Ltd
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package bi.meteorite.sentiment;

import bi.meteorite.sentiment.nlp.SentimentClass;
import bi.meteorite.sentiment.nlp.rollup.Sentence;
import bi.meteorite.sentiment.nlp.rollup.SentimentRollup;
import edu.stanford.nlp.ie.machinereading.structure.AnnotationUtils;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.ejml.simple.SimpleMatrix;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This class is part of the demo step plug-in implementation.
 * It demonstrates the basics of developing a plug-in step for PDI. 
 * 
 * The demo step adds a new string field to the row stream and sets its
 * value to "Hello World!". The user may select the name of the new field.
 *   
 * This class is the implementation of StepInterface.
 * Classes implementing this interface need to:
 * 
 * - initialize the step
 * - execute the row processing logic
 * - dispose of the step 
 * 
 * Please do not create any local fields in a StepInterface class. Store any
 * information related to the processing logic in the supplied step data interface
 * instead.  
 * 
 */

public class NLPStep extends BaseStep implements StepInterface {
    private static Class<?> PKG = NLPStepMeta.class; // for i18n purposes, needed by Translator2!!
    private NLPStepData data;
    private NLPStepMeta meta;
	/**
	 * The constructor should simply pass on its arguments to the parent class.
	 * 
	 * @param s 				step description
	 * @param stepDataInterface	step data class
	 * @param c					step copy
	 * @param t					transformation description
	 * @param dis				transformation executing
	 */
	public NLPStep(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s, stepDataInterface, c, t, dis);
	}
	

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		// Casting to step-specific implementation classes is safe
		meta = (NLPStepMeta) smi;
		data = (NLPStepData) sdi;

        return super.init(meta, data);
    }

    private String processString( String document){
        // shut off the annoying intialization messages
        Properties props = new Properties();
        //specify the annotators that we want to use to annotate the text.  We need a tokenized sentence with POS tags to extract sentiment.
        //this forms our pipeline
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        Annotation annotation = pipeline.process(document);
        List<Sentence> sentences = new ArrayList<Sentence>();
        /*
         * We're going to iterate over all of the sentences and extract the sentiment.  We'll adopt a majority rule policy
         */
        for( CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class))
        {
            //for each sentence, we get the sentiment that CoreNLP thinks this sentence indicates.
            Tree sentimentTree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
            int sentimentClassIdx = RNNCoreAnnotations.getPredictedClass(sentimentTree);
            SentimentClass sentimentClass = SentimentClass.getSpecific(sentimentClassIdx);

            /*
             * Each possible sentiment has an associated probability, so let's pull the entire
             * set of probabilities across all sentiment classes.
             */
            double[] probs = new double[SentimentClass.values().length];
            {
                SimpleMatrix mat = RNNCoreAnnotations.getPredictions(sentimentTree);
                for(int i = 0;i < SentimentClass.values().length;++i)
                {
                    probs[i] = mat.get(i);
                }
            }
            /*
             * Add the sentence and the associated probabilities to our list.
             */
            String sentenceStr = AnnotationUtils.sentenceToString(sentence).replace("\n", "");
            sentences.add(new Sentence(probs, sentenceStr, sentimentClass));
        }
        SentimentClass sentimentClass = null;
        if(meta.getAnalysisType().equals("Wilson Score")) {
             sentimentClass = SentimentRollup.WILSON_SCORE.apply(sentences);
        }
        else if(meta.getAnalysisType().equals("Simple Vote Rollup")) {
            sentimentClass = SentimentRollup.SIMPLE_VOTE.apply(sentences);
        }
        else if(meta.getAnalysisType().equals("Longest Sentence Wins")) {
            sentimentClass = SentimentRollup.LONGEST_SENTENCE_WINS.apply(sentences);
        }
        else if(meta.getAnalysisType().equals("Last Sentence Wins")) {
            sentimentClass = SentimentRollup.LAST_SENTENCE_WINS.apply(sentences);
        }
        else if(meta.getAnalysisType().equals("Average Probabilities Rollup")) {
            sentimentClass = SentimentRollup.AVERAGE_PROBABILITIES.apply(sentences);
        }


        if (sentimentClass != null) {
            return sentimentClass.toString();
        }
        else return null;
    }

    private Object[] processRow( RowMetaInterface rowMeta, Object[] row ) throws KettleException {

        Object[] RowData = new Object[data.outputRowMeta.size()];
        // Copy the input fields.
        System.arraycopy( row, 0, RowData, 0, rowMeta.size() );
        int j = 0; // Index into "new fields" area, past the first {data.inputFieldsNr} records
        for ( int i = 0; i < data.nrFieldsInStream; i++ ) {
            if ( data.inStreamNrs[i] >= 0 ) {
                // Get source value
                String value = getInputRowMeta().getString( row, data.inStreamNrs[i] );
                // Apply String operations and return result value
                value =
                        processString( value );
                if ( Const.isEmpty(data.outStreamNrs[i]) ) {
                    // Update field
                    RowData[data.inStreamNrs[i]] = value;
                    data.outputRowMeta.getValueMeta( data.inStreamNrs[i] )
                            .setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
                } else {
                    // create a new Field
                    RowData[data.inputFieldsNr + j] = value;
                    j++;
                }
            }
        }
        return RowData;
    }

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

		NLPStepMeta meta = (NLPStepMeta) smi;
		NLPStepData data = (NLPStepData) sdi;

		Object[] r = getRow();

		if (r == null){
			setOutputDone();
			return false;
		}

		if (first) {
			first = false;
            data.outputRowMeta = getInputRowMeta().clone();
            data.inputFieldsNr = data.outputRowMeta.size();
            meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
            data.nrFieldsInStream = meta.getFieldInStream().length;
            data.inStreamNrs = new int[data.nrFieldsInStream];

			// use meta.getFields() to change it, so it reflects the output row structure
			//meta.getFields(data.outputRowMeta, getStepname(), null, null, this, null, null);
            for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
                data.inStreamNrs[i] = getInputRowMeta().indexOfValue( meta.getFieldInStream()[i] );
                if ( data.inStreamNrs[i] < 0 ) { // couldn't find field!

                    throw new KettleStepException( BaseMessages.getString(PKG, "StringOperations.Exception.FieldRequired", meta
                            .getFieldInStream()[i]) );
                }
                // check field type
                if ( !getInputRowMeta().getValueMeta( data.inStreamNrs[i] ).isString() ) {
                    throw new KettleStepException( BaseMessages.getString( PKG, "StringOperations.Exception.FieldTypeNotString",
                            meta.getFieldInStream()[i] ) );
                }
            }

            data.outStreamNrs = new String[data.nrFieldsInStream];
            for ( int i = 0; i < meta.getFieldInStream().length; i++ ) {
                data.outStreamNrs[i] = meta.getFieldOutStream()[i];
            }
		}
        Object[] output = processRow( getInputRowMeta(), r );

		putRow(data.outputRowMeta, output);

		// log progress if it is time to to so
		if (checkFeedback(getLinesRead())) {
			logBasic("Linenr " + getLinesRead()); // Some basic logging
		}

		return true;
	}

	/**
	 * This method is called by PDI once the step is done processing.
	 *
	 * The dispose() method is the counterpart to init() and should release any resources
	 * acquired for step execution like file handles or database connections.
	 *
	 * The meta and data implementations passed in can safely be cast
	 * to the step's respective implementations.
	 *
	 * It is mandatory that super.dispose() is called to ensure correct behavior.
	 *
	 * @param smi 	step meta interface implementation, containing the step settings
	 * @param sdi	step data interface implementation, used to store runtime information
	 */
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {

		// Casting to step-specific implementation classes is safe
		NLPStepMeta meta = (NLPStepMeta) smi;
		NLPStepData data = (NLPStepData) sdi;
		
		super.dispose(meta, data);
	}

}
