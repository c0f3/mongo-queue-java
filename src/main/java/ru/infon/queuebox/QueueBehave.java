package ru.infon.queuebox;

import java.util.Collection;

/**
 * 06.06.2017
 * @author KostaPC
 * 2017 Infon ZED
 *
 * Class implemented whis interface provide in fact queue storage and storage behave.
 **/
public interface QueueBehave<T> {

    int getThreadsCount();
    int getFetchLimit();
    void put(MessageContainer<T> event);
    Collection<MessageContainer<T>> find(QueueConsumer<T> consumer);
    void remove(MessageContainer<T> packet);
    void reset(MessageContainer<T> packet);

}
