package bi.meteorite.sentiment.nlp;


import bi.meteorite.sentiment.nlp.rollup.Sentence;
import bi.meteorite.sentiment.nlp.rollup.SentimentRollup;
import com.google.common.base.Function;
import edu.stanford.nlp.ie.machinereading.structure.AnnotationUtils;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.ejml.simple.SimpleMatrix;

import java.util.*;

/**
 * Created by cstella on 3/21/14.
 */
public enum SentimentAnalyzer implements Function<String, SentimentClass> {
    INSTANCE;

    public SentimentClass apply(String document)
    {
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

        /*
         * Finally, rollup the score of the entire document given the list of sentiments
         * for each sentence.
         *
         * I have found that this is a surprisingly difficult thing.  We're going to use the fact that
         * we are mapping these documents onto Positive, Negative to use a technique that people use
         * to figure out overall product ratings from individual star ratings, the Wilson Score.
         * See http://www.evanmiller.org/how-not-to-sort-by-average-rating.html.
         *
         * For your convenience, I have implemented a number of other approaches, including:
         *  * Basic majority-rule voting
         *  * Max probability across all sentiments for all sentences
         *  * Last sentence wins
         *  * Longest sentence wins
         *
         */

                return SentimentRollup.WILSON_SCORE.apply(sentences);

    }
}
