package ru.infon.queuebox;

import net.c0f3.queuebox.QueueBoxContext;

import java.util.Collection;

/**
 * 06.06.2017
 * @author KostaPC
 * 2017 Infon ZED
 *
 * Class implemented whis interface provide in fact queue storage and storage behave.
 **/
public interface QueueBehave<T> {

    int getFetchLimit();
    void put(MessageContainer<T> event);
    Collection<MessageContainer<T>> find(QueueConsumer<T> consumer);
    void remove(MessageContainer<T> packet);
    void reset(MessageContainer<T> packet);
    void setContext(QueueBoxContext context);

}
