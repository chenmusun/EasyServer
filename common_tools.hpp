#ifndef COMMON_TOOLS_HPP_
#define COMMON_TOOLS_HPP_
#include"hiredis/hiredis.h"
#include "mongodblib.h"
#include "kafkalib.h"

//redis part
void RegisterTcpConnectionInRedis(const char * key_id// ,const char * imei,const char * iccid
                                  ,const char * ip,time_t ts,const char * serverip,int port)
{
    redisContext *c=NULL;
    void * reply=NULL;
    do{
        c = redisConnect(serverip, port);
        if (c == NULL){
            LOG(ERROR)<<"redisContext allocate failed";
            break;
        }

        if(c->err) {
            LOG(ERROR)<<c->errstr;
            break;
        }

        reply=redisCommand(c,"hmset %s dxptopic %s dxpdmtopic %sdm timestamp %d",key_id,ip,ip,ts);
        LOG(DEBUG)<<"execute cmd : hmset "<<key_id<<" ip "<<ip<<" timestamp "<<ts;

        if(!reply)
        {
            LOG(ERROR)<<"redis issue command error";
            break;
        }

        redisReply * rp=(redisReply *)reply;
        if(rp->type==REDIS_REPLY_ERROR){
            LOG(ERROR)<<rp->str;
            break;
        }

    }while(0);

    freeReplyObject(reply);
    redisFree(c);
}

void UnRegisterTcpConnectionInRedis(const char * key_id,int ts,const char * serverip,int port)
{
    redisContext *c=NULL;
    void * replyWatch=NULL;
    void * replyGet=NULL;
    void * replyDel=NULL;
    do{
        c = redisConnect(serverip, port);
        if (c == NULL){
            LOG(ERROR)<<"redisContext allocate failed";
            break;
        }

        if(c->err) {
            LOG(ERROR)<<c->errstr;
            break;
        }
        //watch
        replyWatch=redisCommand(c,"watch %s",key_id);
        if(!replyWatch)
        {
            LOG(ERROR)<<"redis watch "<<key_id<<" error";
            break;
        }

        redisReply * rp=(redisReply *)replyWatch;
        if(rp->type==REDIS_REPLY_ERROR){
            LOG(ERROR)<<rp->str;
            break;
        }

        //get
        replyGet=redisCommand(c,"hget %s timestamp",key_id);
        if(!replyGet)
        {
            LOG(ERROR)<<"redis hget "<<key_id<<" error";
            break;
        }

        if(((redisReply *)replyGet)->type!=REDIS_REPLY_STRING)
        {
            LOG(ERROR)<<"redis hget "<<key_id<<" timestamp error and type is "<<((redisReply *)replyGet)->type;
                break;
        }

        //use transaction here
        int timestamp=atoi(((redisReply *)replyGet)->str);
        // LOG(ERROR)<<<<endl;
        if(timestamp==ts){
            redisAppendCommand(c,"multi");
            redisAppendCommand(c,"hdel %s dxptopic dxpdmtopic timestamp",key_id);
            redisAppendCommand(c,"exec");
            redisGetReply(c,&replyDel); // reply for multi
            freeReplyObject(replyDel);
            redisGetReply(c,&replyDel); // reply for hdel
            freeReplyObject(replyDel);
            redisGetReply(c,&replyDel); // reply for exec
            // freeReplyObject(replyExec);

            if(!replyDel)
            {
                LOG(ERROR)<<"redis hdel "<<key_id<<" error";
                break;
            }

            rp=(redisReply *)replyDel;
            if(rp->type==REDIS_REPLY_ERROR){
                LOG(ERROR)<<rp->str;
                break;
            }


            if(rp->type!=REDIS_REPLY_NIL)
                LOG(DEBUG)<<"execute cmd : hdel "<<key_id<<" timestamp is "<<ts;
        }

    }while(0);

    freeReplyObject(replyWatch);
    freeReplyObject(replyGet);
    freeReplyObject(replyDel);
    redisFree(c);
}

std::tuple<std::string,std::string,int> GetDeviceTopicFromRedis(const char * key_id,const char * serverip,int port)
{
    redisContext *c=NULL;
    redisReply * reply=NULL;
    std::string ret;
    std::string ret2;
    int retint=-1;
    do{
        c = redisConnect(serverip, port);
        if (c == NULL){
            LOG(ERROR)<<"redisContext allocate failed";
            break;
        }

        if(c->err) {
            LOG(ERROR)<<c->errstr;
            break;
        }

        //get
        reply=(redisReply *)redisCommand(c,"hmget %s fleet-uid dm-topic partition",key_id);
        if(!reply||reply->type!=REDIS_REPLY_ARRAY)
        {
            LOG(ERROR)<<"redis hmget "<<key_id<<" error";
            break;
        }

        if((reply->element[0]->type!=REDIS_REPLY_STRING)||(reply->element[1]->type!=REDIS_REPLY_STRING)||(reply->element[2]->type!=REDIS_REPLY_STRING)){
            LOG(ERROR)<<"redis hmget "<<key_id<<" error";
            break;
        }

        ret=reply->element[0]->str;
        ret2=reply->element[1]->str;
        retint=std::stoi(reply->element[2]->str);
    }while(0);

    freeReplyObject(reply);
    redisFree(c);
    return std::make_tuple(ret,ret2,retint);
}


#endif
