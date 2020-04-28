package net.c0f3.queuebox;

public interface QueueStatistic {
    void increment(String key);

    String getValue(String statFindCounter);

    class VoidStatistic implements QueueStatistic {
        @Override
        public void increment(String key) {}

        @Override
        public String getValue(String statFindCounter) {
            return null;
        }
    }

    VoidStatistic voidStatistic = new VoidStatistic();

    static VoidStatistic voidInstance() {
        return voidStatistic;
    }
}
