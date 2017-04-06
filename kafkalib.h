#ifndef KAFKA_LIB_H_
#define KAFKA_LIB_H_
/* #include <unordered_map>*/
#include <string>
#include<vector>
#include<memory>
#include<thread>
/* #include <mutex> */
#include "librdkafka/rdkafka.h"
typedef void (*consume_cb) (rd_kafka_message_t *rkmessage,void *opaque);

class KafkaLib{
public:
        bool InitKafkaLib(const char * brokers,const char * id);
        int ProduceMessage(const std::string& topicname,int msgflags,void *message, size_t len,const void * key,int keylen,int partition=-1);
        static void ConsumeMessage(KafkaLib * klib,const std::string& topicname,consume_cb cb,void * arg);
        /* static void SetConsumeCB(consume_cb cb,void * arg) */
        /* { */
        /*         concb_=cb; */
        /*         arg_=arg; */
        /* } */
        bool StartConsumeMessage(const std::string& topicname,consume_cb cb,void * arg);
        bool InitProducer(const char * brokers);
        bool InitConsumer(const char * brokers,const char * id);
        rd_kafka_topic_t * GetProducerTopic(const std::string& topicname);
        rd_kafka_topic_t * GetConsumerTopic(const std::string& topicname);
        /* std::unordered_map<std::string,rd_kafka_topic_t *> topic_hash_map_; */
        /* std::mutex topic_hash_map_mutex_; */
        rd_kafka_t *  produce_kafka_;
        rd_kafka_t *  consume_kafka_;
        /* static consume_cb concb_; */
        /* static void * arg_; */
        std::vector<std::shared_ptr<std::thread> > vec_thread_;
        /* std::string kafka_brokers_; */

};
#endif
