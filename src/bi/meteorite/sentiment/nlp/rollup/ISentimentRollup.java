package bi.meteorite.sentiment.nlp.rollup;

import bi.meteorite.sentiment.nlp.SentimentClass;
import com.google.common.base.Function;

import java.util.List;

/**
 * Created by cstella on 3/25/14.
 */
public interface ISentimentRollup extends Function<List<Sentence>, SentimentClass>
{
}