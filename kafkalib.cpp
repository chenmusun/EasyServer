#include"kafkalib.h"
// #include<iostream>
#include<string.h>
//增加日志功能
#define ELPP_THREAD_SAFE
#include"easylogging++.h"
// #include"libevent_server.h"
// #include"servicedatastructs.hpp"
// extern LibeventServer * g_libevent_server;
// using namespace std;

//*************************************************************************************
//kafka
//*************************************************************************************
// consume_cb KafkaLib::concb_=NULL;
// void * KafkaLib::arg_=NULL;

static void msg_delivered(rd_kafka_t *rk,const rd_kafka_message_t *rkmessage, void *opaque) {
        // if(rkmessage->err){
        //         char tmp[100]={0};
        //         memcpy(tmp,rkmessage->payload,rkmessage->len);
        //         LOG(WARNING)<<tmp;
        // }
}

bool KafkaLib::InitKafkaLib(const char * brokers,const char * id)
{
        if(InitProducer(brokers)&&InitConsumer(brokers,id))
                return true;
        else
                return false;

}

int KafkaLib::ProduceMessage(const std::string&topicname,int msgflags,void *message, size_t len,const void * key,int keylen,int partition)
{
        int ret=-1;
        do{
                rd_kafka_topic_t * topic=GetProducerTopic(topicname);
                if(!topic)
                        break;

                if(rd_kafka_produce(topic,partition,
                                         msgflags,
                                         /* Payload and length */
                                         message, len,
                                         /* Optional key and its length */
                                         key, keylen,
                                         /* Message opaque, provided in
                                          * delivery report callback as
                                          * msg_opaque. */
                                         NULL)==-1){
                        rd_kafka_poll(produce_kafka_,0);
                        break;
                }

                /* Poll to handle delivery reports */
                rd_kafka_poll(produce_kafka_, 0);

                ret=0;
        }while (0);

        LOG(DEBUG)<<"kafka out queue len :"<<rd_kafka_outq_len(produce_kafka_);
        return ret;
}

bool KafkaLib::InitProducer(const char * brokers){
       bool ret=false;
        do{
                rd_kafka_conf_t *myconf;
                rd_kafka_conf_res_t res;

                myconf = rd_kafka_conf_new();
                res = rd_kafka_conf_set(myconf, "socket.timeout.ms", "600",NULL, 0);
                if (res != RD_KAFKA_CONF_OK){
                    LOG(ERROR)<<"rd_kafka_conf_set error ";
                        // std::cout<<"rd_kafka_conf_set error ";
                    break;
                }

                res = rd_kafka_conf_set(myconf, "queue.buffering.max.messages", "1000000",NULL, 0);
                if (res != RD_KAFKA_CONF_OK)
                {
                    LOG(ERROR)<<"rd_kafka_conf_set error ";
                        // std::cout<<"rd_kafka_conf_set error ";
                    break;
                }

                //发消息时回调
                rd_kafka_conf_set_dr_msg_cb(myconf, msg_delivered);
                produce_kafka_=rd_kafka_new(RD_KAFKA_PRODUCER,myconf,NULL,0);

                //brokers add
                if(produce_kafka_)
                {
                        rd_kafka_brokers_add(produce_kafka_,brokers);
                        ret=true;
                }

        }while(0);
        return ret;
}

bool KafkaLib::InitConsumer(const char * brokers,const char * id){
       bool ret=false;
        do{
                rd_kafka_conf_t *myconf;
                rd_kafka_conf_res_t res;

                myconf = rd_kafka_conf_new();
                res=rd_kafka_conf_set(myconf,"group.id",id,NULL,0);
                res = rd_kafka_conf_set(myconf, "socket.timeout.ms", "600",NULL, 0);
                if (res != RD_KAFKA_CONF_OK){
                    LOG(ERROR)<<"rd_kafka_conf_set error ";
                        // std::cout<<"rd_kafka_conf_set error ";
                    break;
                }

                consume_kafka_=rd_kafka_new(RD_KAFKA_CONSUMER,myconf,NULL,0);

                //brokers add
                if(consume_kafka_)
                {
                        rd_kafka_brokers_add(consume_kafka_,brokers);
                        ret=true;
                }

        }while(0);
        return ret;

}


rd_kafka_topic_t * KafkaLib::GetProducerTopic(const std::string& topicname)
{
        //rd_kafka_topic_conf_new() 有内部引用计数
        rd_kafka_topic_t *ret=NULL;
        rd_kafka_topic_conf_t * topicconf = rd_kafka_topic_conf_new();
        ret=rd_kafka_topic_new(produce_kafka_,topicname.c_str(),topicconf);
        return ret;
}

rd_kafka_topic_t * KafkaLib::GetConsumerTopic(const std::string& topicname)
{
        rd_kafka_topic_t *ret=NULL;
        rd_kafka_topic_conf_t * topicconf = rd_kafka_topic_conf_new();
        rd_kafka_topic_conf_set(topicconf,"auto.commit.interval.ms","10",NULL,0);
        ret=rd_kafka_topic_new(consume_kafka_,topicname.c_str(),topicconf);
        return ret;
}

void KafkaLib::ConsumeMessage(KafkaLib * klib,const std::string& topicname,consume_cb cb,void * arg)
{
        rd_kafka_topic_t * topic=klib->GetConsumerTopic(topicname);
        if(!topic){
                LOG(WARNING)<<"Get Consumer topic failed!";
                return;
        }

        if(rd_kafka_consume_start(topic,0, RD_KAFKA_OFFSET_STORED)){
                LOG(WARNING)<<"rd_kafka_consume_start failed";
                return;
        }

        while(1){
                if(rd_kafka_consume_callback(topic,0,-1,cb,// g_libevent_server
                                             arg)==-1)
                        LOG(WARNING)<<"rd_kafka_consume_callback error";
        }

}

bool KafkaLib::StartConsumeMessage(const std::string& topicname,consume_cb cb,void * arg)
{
        bool ret=false;
        try{
                std::shared_ptr<std::thread> threadptr(new std::thread(KafkaLib::ConsumeMessage,this,topicname,cb,arg));
                vec_thread_.push_back(threadptr);
                ret=true;
        }
        catch(...)
        {
        }
        return ret;
}
