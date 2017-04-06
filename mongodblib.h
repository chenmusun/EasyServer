#ifndef MONGODB_LIB_H_
#define MONGODB_LIB_H_
#include "libmongoc-1.0/mongoc.h"
#include "libbson-1.0/bson.h"
#include<mutex>
#include<list>
#include<queue>
#include<condition_variable>
#include<memory>
#include<thread>

struct MongoMsg
{
    MongoMsg(){

    }
MongoMsg(const std::string& m,const std::string& d,const std::string& c)
:msg(m),db(d),col(c)
    {
    }
    std::string msg;
    std::string db;
    std::string col;
};
struct MongodbLib{
    MongodbLib()
        {
            pool_=NULL;
            mongoc_init ();
        }
    ~MongodbLib()
        {
            if(pool_)
                mongoc_client_pool_destroy (pool_);
            mongoc_cleanup ();
        }

    bool init(const std::string& servierurl){
        bool ret=false;
        do
        {
            mongoc_uri_t * uri = mongoc_uri_new (servierurl.c_str());
            pool_ = mongoc_client_pool_new (uri);
            mongoc_uri_destroy (uri);
            if(!pool_)
                break;
            ret=true;
        }while (0) ;
        return ret;
    }

    static void InsertDataIntoMongoDB(MongodbLib * lib,const std::string& db,const std::string& collname,const std::string& jsonstr);
    static void InsertDataIntoMongoDBQueue(MongodbLib * lib,const std::string& db,const std::string& collname,const std::string& jsonstr);
    static void ConsumeMessageFromQueue(MongodbLib* lib);
    bool StartConsumeMessage(MongodbLib* lib,int num);
    void ShrinkQueueToFit()
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            std::queue<MongoMsg > msgstmp(msgs_);
            /* std::queue< std::function<void()> > taskstmp(tasks); */
            msgstmp.swap(msgs_);
        }
    mongoc_client_pool_t * pool_;
    int GetMongoQueueSize(){
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return msgs_.size();
    }

    void ClearMongoQueue(){
        std::lock_guard<std::mutex> lock(queue_mutex_);
        std::queue<MongoMsg > msgstmp;
        /* std::queue< std::function<void()> > taskstmp(tasks); */
        msgstmp.swap(msgs_);
    }
    /* std::queue<MongoMsg,std::list<MongoMsg> > msgs_; */
    std::queue<MongoMsg> msgs_;
    std::mutex queue_mutex_;
    std::condition_variable queue_not_empty_;
    std::vector<std::shared_ptr<std::thread> > vec_thread_;
        /* std::queue< std::function<void()>,std::list< std::function<void()> > > tasks; */
};

#endif
