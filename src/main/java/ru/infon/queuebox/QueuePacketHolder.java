package ru.infon.queuebox;

import java.util.Collection;

/**
 * 03.04.2017
 * @author KostaPC
 * 2017 Infon ZED
 **/
public interface QueuePacketHolder<T> {

    int getFetchLimit();
    Collection<MessageContainer<T>> fetch(QueueConsumer<T> consumer);
    void ack(MessageContainer<T> packet);
    void reset(MessageContainer<T> packet);

}
