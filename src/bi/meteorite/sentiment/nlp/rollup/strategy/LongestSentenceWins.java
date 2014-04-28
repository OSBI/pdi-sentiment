package bi.meteorite.sentiment.nlp.rollup.strategy;


import bi.meteorite.sentiment.nlp.SentimentClass;
import bi.meteorite.sentiment.nlp.rollup.ISentimentRollup;
import bi.meteorite.sentiment.nlp.rollup.Sentence;

import java.util.List;

/**
 * Created by cstella on 3/26/14.
 */
public class LongestSentenceWins implements ISentimentRollup {

    @Override
    public SentimentClass apply(List<Sentence> input) {
        int length = 0;
        Sentence actualSentence = null;
        for(Sentence in : input)
        {
            if(in.getSentence().length() > length)
            {
                actualSentence = in;
            }
        }
        return actualSentence.getSentiment();
    }
}