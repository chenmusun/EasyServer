#include"mongodblib.h"
#include"easylogging++.h"

void MongodbLib::InsertDataIntoMongoDB(MongodbLib * lib,const std::string& db,const std::string& collname,const std::string& jsonstr)
{
        mongoc_client_pool_t* pool=lib->pool_;
        mongoc_client_t *client=NULL;
        mongoc_collection_t *collection=NULL;
        bson_t  *bson=NULL;
        bson_error_t error;

        do{
                client= mongoc_client_pool_pop (pool);
                if(!client){
                        LOG(ERROR)<<"mongodb get client error";
                        break;
                }

                collection = mongoc_client_get_collection (client, db.c_str(),collname.c_str());
                if(!collection)
                {
                        LOG(ERROR)<<"mongodb get collection error";
                        break;
                }


                bson = bson_new_from_json ((const uint8_t *)jsonstr.c_str(),jsonstr.length(),NULL);
                if(!bson){
                        LOG(ERROR)<<"mongodb create bson from json failed";
                        break;
                }

                // mongoc_write_concern_t concern(MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED);

                if(!mongoc_collection_insert (collection, MONGOC_INSERT_NONE, bson, NULL, &error))
                {
                        LOG(ERROR)<<"mongodb insert data failed and message is "<<error.message;
                        break;
                }

        }while (0);
        if(client)
                mongoc_client_pool_push (pool, client);
        if(collection)
                mongoc_collection_destroy (collection);
        if(bson)
                bson_destroy(bson);
}

void MongodbLib::InsertDataIntoMongoDBQueue(MongodbLib* lib, const std::string& db, const std::string& collname, const std::string& jsonstr)
{
        {
                std::lock_guard<std::mutex> lock(lib->queue_mutex_);
                lib->msgs_.emplace(MongoMsg(jsonstr,db,collname));
        }

        lib->queue_not_empty_.notify_one();
}


void MongodbLib::ConsumeMessageFromQueue(MongodbLib* lib)
{
        while (true) {
                MongoMsg msg;
                int msgsize=0;
                {
                        std::unique_lock<std::mutex> lock(lib->queue_mutex_);
                        lib->queue_not_empty_.wait(lock,[lib]{return !lib->msgs_.empty();});
                        msg=lib->msgs_.front();
                        lib->msgs_.pop();
                        // msgsize=lib->msgs_.size();

                }
                InsertDataIntoMongoDB(lib,msg.db,msg.col,msg.msg);
                // LOG(DEBUG)<<"mongodb size "<<msgsize;
                // lib->msgs_.emplace(MongoMsg(jsonstr,db,collname));
        }
}

bool MongodbLib::StartConsumeMessage(MongodbLib* lib,int num)
{
        bool ret=false;
        try{
                for(int i=0;i<num;++i){
                        std::shared_ptr<std::thread> threadptr(new std::thread(MongodbLib::ConsumeMessageFromQueue,lib));
                        vec_thread_.push_back(threadptr);
                }

                ret=true;
        }
        catch(...)
        {
        }
        return ret;
}
