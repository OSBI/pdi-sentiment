package bi.meteorite.sentiment.nlp.rollup.strategy;

import bi.meteorite.sentiment.nlp.SentimentClass;
import bi.meteorite.sentiment.nlp.rollup.ISentimentRollup;
import bi.meteorite.sentiment.nlp.rollup.Sentence;

import java.util.List;

/**
 * Created by cstella on 3/26/14.
 */
public class LastSentenceWins implements ISentimentRollup {

    @Override
    public SentimentClass apply(List<Sentence> input) {
        return input.get(input.size() - 1).getSentiment();
    }
}