package bi.meteorite.sentiment.nlp.rollup;

import bi.meteorite.sentiment.nlp.SentimentClass;
import bi.meteorite.sentiment.nlp.rollup.strategy.*;
import bi.meteorite.sentiment.nlp.rollup.strategy.AverageProbabilitiesRollup;
import bi.meteorite.sentiment.nlp.rollup.strategy.LastSentenceWins;
import bi.meteorite.sentiment.nlp.rollup.strategy.LongestSentenceWins;
import bi.meteorite.sentiment.nlp.rollup.strategy.SimpleVoteRollup;

import java.util.List;

/**
 * Created by cstella on 3/25/14.
 */
public enum SentimentRollup implements ISentimentRollup {
    SIMPLE_VOTE(new SimpleVoteRollup())
    ,AVERAGE_PROBABILITIES(new AverageProbabilitiesRollup())
    , LAST_SENTENCE_WINS(new LastSentenceWins())
    , LONGEST_SENTENCE_WINS(new LongestSentenceWins())
    , WILSON_SCORE(new WilsonScore())
    ;

    private ISentimentRollup proxy;
    SentimentRollup(ISentimentRollup proxy) { this.proxy = proxy;}

    @Override
    public SentimentClass apply(List<Sentence> input) {
        return proxy.apply(input);
    }
}