package dj.flink;

/**
 * @author: Dj
 * @Description:
 * @date 2020年01月31日 14:37:10
 */
public class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {
    }

    public WordWithCount(String word, long l) {
        this.count = l;
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordWithCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
