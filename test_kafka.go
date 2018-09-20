package main

import (
    "fmt"
    "time"
    "math/rand"
    "strconv"
    "github.com/Shopify/sarama"
    cluster "github.com/sarama-cluster"
)

var (
    topics = "my_test_topic"
    brokers = []string{"127.0.0.1:9092", "10.192.168.1:9092"}
)

// consumer 消费者
func consumer() {
    groupID := "my_group"
    config := cluster.NewConfig()
    config.Group.Return.Notifications = true
    config.Consumer.Offsets.CommitInterval = 1 * time.Second
    config.Consumer.Offsets.Initial = sarama.OffsetNewest //初始从最新的offset开始

    c, err := cluster.NewConsumer(brokers, groupID, []string{topics}, config)
    if err != nil {
        fmt.Println("failed open consumer: ", err)
        return
    }
    defer c.Close()
    go func(c *cluster.Consumer) {
        errors := c.Errors()
        noti := c.Notifications()
        for {
            select {
                case err := <-errors:
                    fmt.Println("consumer: ", err)
                    if err == nil {
                        return
                    }
                case ev := <- noti:
                    fmt.Println("event notification: ", ev)
            }
        }
    }(c)

    for msg := range c.Messages() {
        fmt.Printf("consumer receive msgs: %s/%d/%d\t/%s:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
        c.MarkOffset(msg, "") //MarkOffset 并不是实时写入kafka，有可能在程序crash时丢掉未提交的offset
    }
}

//syncProducer 同步生产者
//并发量小时采用的方式
func syncProducer() {
    config := sarama.NewConfig()
    //config.Producer.RequiredAcks = sarama.WaitForAll
    //config.Producer.Partitioner = sarama.NewRandomPartitioner
    config.Producer.Return.Successes = true
    config.Producer.Timeout = 5 * time.Second
    p, err := sarama.NewSyncProducer(brokers, config)
    defer p.Close()
    if err != nil {
        fmt.Println("new sync producer err: ", err)
        return
    }
    v := "sync: " + strconv.Itoa(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10000))
    fmt.Println("sync producer: ", v)
    msg := &sarama.ProducerMessage{
        Topic: topics,
        Value: sarama.ByteEncoder(v),
    }
    if _, _, err := p.SendMessage(msg); err != nil {
        fmt.Println("sync producer send msg err: ", err)
        return
    }
}

//asyncProducer 异步生产者
//并发量大时采用的方式
func asyncProducer() {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    config.Producer.Timeout = 5 * time.Second
    p, err := sarama.NewAsyncProducer(brokers, config)
    defer p.Close()
    if err != nil {
        fmt.Println("new async producer err: ", err)
        return
    }
    go func(ap sarama.AsyncProducer) {
        errors := ap.Errors()
        success := ap.Successes()
        for {
            select {
            case err := <-errors:
                if err != nil {
                    fmt.Println("async producer: ", err)
                } else {
                    fmt.Println("async no err...")
                }
            case <- success:
                    fmt.Println("async success...")
            }
        }
    }(p)
     
    v := "async: " + strconv.Itoa(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10000))
    fmt.Println("sync producer: ", v)
    msg := &sarama.ProducerMessage{
        Topic: topics,
        Value: sarama.ByteEncoder(v),
    }
    p.Input() <- msg
}

func main() {
    //syncProducer()
    asyncProducer()
    //consumer()
}
