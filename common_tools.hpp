#ifndef COMMON_TOOLS_HPP_
#define COMMON_TOOLS_HPP_
#include"hiredis/hiredis.h"
#include "mongodblib.h"
#include "kafkalib.h"
#include <sys/mman.h>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/locks.hpp>

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

std::tuple<std::string,std::string,int,std::string,std::string> GetDeviceTopicFromRedis(const char * key_id,const char * serverip,int port)
{
    redisContext *c=NULL;
    redisReply * reply=NULL;
    std::string ret;
    std::string ret2;
    std::string ret3;
    std::string ret4;

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
        reply=(redisReply *)redisCommand(c,"hmget %s fleet-uid dm-topic partition targetfwversion targetbinfile",key_id);
        if(!reply||reply->type!=REDIS_REPLY_ARRAY)
        {
            LOG(ERROR)<<"redis hmget "<<key_id<<" error not array !";
            break;
        }

        if((reply->element[0]->type!=REDIS_REPLY_STRING)||(reply->element[1]->type!=REDIS_REPLY_STRING)||(reply->element[2]->type!=REDIS_REPLY_STRING)||(reply->element[3]->type!=REDIS_REPLY_STRING)||(reply->element[4]->type!=REDIS_REPLY_STRING)){
            LOG(ERROR)<<"redis hmget "<<key_id<<" error not string";
            break;
        }

        ret=reply->element[0]->str;
        ret2=reply->element[1]->str;
        retint=std::stoi(reply->element[2]->str);
        ret3=reply->element[3]->str;
        ret4=reply->element[4]->str;
    }while(0);

    freeReplyObject(reply);
    redisFree(c);
    return std::make_tuple(ret,ret2,retint,ret3,ret4);
}

std::tuple<std::string,std::string,int,std::string,std::string> GetMxpDeviceTopicFromRedis(const char * key_id,const char * key_set_id,const char * serverip,int port,std::vector<std::string>& vec)
{
    redisContext *c=NULL;
    void * vreply=NULL;
    void * vreply2=NULL;
    std::string ret;
    std::string ret2;
    std::string ret3;
    std::string ret4;

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
        redisAppendCommand(c,"hmget %s fleet-uid dm-topic partition targetfwversion targetbinfile",key_id);
        redisAppendCommand(c,"spop %s 10",key_set_id);
        redisGetReply(c,&vreply); // reply for hmget

        redisReply * reply=(redisReply *)vreply;
        if(!reply||reply->type!=REDIS_REPLY_ARRAY)
        {
            LOG(ERROR)<<"redis hmget "<<key_id<<" error not array !";
            break;
        }

        if((reply->element[0]->type!=REDIS_REPLY_STRING)||(reply->element[1]->type!=REDIS_REPLY_STRING)||(reply->element[2]->type!=REDIS_REPLY_STRING)||(reply->element[3]->type!=REDIS_REPLY_STRING)||(reply->element[4]->type!=REDIS_REPLY_STRING)){
            LOG(ERROR)<<"redis hmget "<<key_id<<" error not string";
            break;
        }

        ret=reply->element[0]->str;
        ret2=reply->element[1]->str;
        retint=std::stoi(reply->element[2]->str);
        ret3=reply->element[3]->str;
        ret4=reply->element[4]->str;

        redisGetReply(c,&vreply2); // reply for smembers
        redisReply * reply2=(redisReply *)vreply2;
        if(!reply2||reply2->type!=REDIS_REPLY_ARRAY)
        {
            LOG(ERROR)<<"redis spop "<<key_set_id<<" error not array !";
            break;
        }

        int elements=reply2->elements;
        for(int i=0;i<elements;++i){
            if(reply2->element[i]->type==REDIS_REPLY_STRING)
                vec.push_back(reply2->element[i]->str);
        }

    }while(0);

    freeReplyObject(vreply);
    freeReplyObject(vreply2);
    redisFree(c);
    return std::make_tuple(ret,ret2,retint,ret3,ret4);
}

//file mmap
struct UpdateDataPacket{
    UpdateDataPacket(){
        addr=NULL;
        packetsize=0;
        slicesize=0;
        slicenum=0;
        crc=0;
    }
        void * addr;
        int packetsize;
        int slicesize;
        int slicenum;
        unsigned int crc;
};

void MmapUpdatePacket(const std::string& packetfile,int slicesize,UpdateDataPacket& packet,bool iscrc32=false)
{
        int fd=-1;
        void * addr=NULL;
        int mmaplen=0;
        unsigned int crc=0;

        do{
                fd=open(packetfile.c_str(),O_RDONLY);
                if(fd<0)
                        break;

                struct stat stat_data;
                if(fstat(fd,&stat_data)<0)
                {
                    LOG(ERROR)<<" fstat error !";
                        break;
                }

                mmaplen=stat_data.st_size;
                if(mmaplen<=0){
                    LOG(ERROR)<<"mmap len error !";
                                break;
                }

                addr=mmap(0,stat_data.st_size,PROT_READ,MAP_PRIVATE,fd,0);
                if(addr==MAP_FAILED){
                    LOG(ERROR)<<"mmap error !";
                        addr=NULL;
                        break;
                }

                unsigned char * paddr=(unsigned char *)addr;
                //crc
                if(iscrc32){
                    // boost::crc_optimal<32, 0x04C11DB7,0xFFFFFFFF, 0, false, false> crc_ccitt;
                    // crc_ccitt.process_bytes(addr,mmaplen);
                    // crc=crc_ccitt.checksum();
                    crc=paddr[0]+paddr[1]*256+paddr[2]*256*256+paddr[3]*256*256*256;
                }
                else {
                    boost::crc_optimal<16, 0x1021,0, 0, false, false>  crc_ccitt;
                    crc_ccitt.process_bytes(addr,mmaplen);
                    crc=crc_ccitt.checksum();
                }

        }while (0);

        if(fd>0)
                close(fd);

        packet.addr=addr;
        packet.packetsize=mmaplen;
        packet.slicesize=slicesize;
        packet.slicenum=mmaplen/slicesize+(mmaplen%slicesize?1:0);
        packet.crc=crc;
}

void UnmmapUpdatePacket(UpdateDataPacket& packet)
{
        if(packet.addr&&packet.packetsize)
                munmap(packet.addr,packet.packetsize);
}

std::tuple<unsigned char *,int> GetSliceDataByIndex(const UpdateDataPacket& packet,int index)
{
    unsigned char * addr=NULL;
    int len=0;
    if(index<=packet.slicenum-1){
        addr=(unsigned char *)packet.addr+index*packet.slicesize;
        len=(index==packet.slicenum-1?packet.packetsize-index*packet.slicesize:packet.slicesize);
    }
    return std::make_tuple(addr,len);
}

boost::shared_mutex mutex_un_map_update_bin;
std::unordered_map<std::string,UpdateDataPacket > un_map_update_bin;
static void InsertUpdateBinIntoMap(const std::string& filename,const UpdateDataPacket& packet){
    std::unique_lock< boost::shared_mutex > lock(mutex_un_map_update_bin);
    auto pos=un_map_update_bin.insert(std::make_pair(filename,packet));
    if(!pos.second){
        LOG(WARNING)<<"update packet changes!";
        pos.first->second=packet;
    }
}

static void DeleteUpdateBinFromMap(const std::string& filename){
    std::unique_lock< boost::shared_mutex > lock(mutex_un_map_update_bin);
    un_map_update_bin.erase(filename);
}

UpdateDataPacket FindUpdateBinFromMap(const std::string& filename){
    boost::shared_lock<boost::shared_mutex> lock(mutex_un_map_update_bin);
    auto pos= un_map_update_bin.find(filename);
    if(pos!=un_map_update_bin.end()){
        return pos->second;
    }
    else {
        return UpdateDataPacket();
    }
}


static int ConvertHexCharToNum2(char c )/* hex char */
{
    int ret=-1;
    if(c<='9'&&c>='0')
        ret=c-'0';
    else if(c<='F'&&c>='A')
        ret=c-'A'+10;
    else if(c<='f'&&c>='a')
        ret=c-'a'+10;
    else{

    }

    return ret;
}


unsigned char * ConvertHexString2Bytes(const char * bytes,unsigned int len,unsigned char * out)
{
    if(len%2){
        return NULL;
    }

    for(int i=0;i<len/2;++i)
    {
        out[i]=ConvertHexCharToNum2(bytes[2*i])*16;
        out[i]+=ConvertHexCharToNum2(bytes[2*i+1]);
    }

    return out;
}
#endif
