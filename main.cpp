#include<iostream>
#include<fstream>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include<boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include"mxphandle.hpp"
// #include"mxpdecode.hpp"
#include"dxphandle.hpp"
using namespace std;
namespace pt = boost::property_tree;//
INITIALIZE_EASYLOGGINGPP//初始化日志记录库

std::string g_server_address;
std::string g_redis_server_ip;
std::string g_api_server_topic;
int g_redis_port;
KafkaLib * g_kafka_lib=NULL;
MongodbLib * g_mongodb_lib=NULL;
std::string g_mongodb_db;
//std::string g_mongodb_coll;
std::string g_mongocoll_deviceinfo;
std::string g_mongocoll_deviceconf;
std::string g_mongocoll_devicelocation;
std::string g_mongocoll_tripdata;
std::string g_mongocoll_towingdata;
std::string g_mongocoll_crashdata;
std::string g_mongocoll_mxp;
std::string g_mongocoll_firstlocatetime;
UpdateDataPacket g_4t48b0;
UpdateDataPacket g_4t48b1;
int g_slice_size;
std::string g_update_bin_path;
std::string g_config_info;

time_t g_logcreatedtime=0;
std::string  GetIpByInterface(const char * interface)
{
        std::string ret;
        struct ifaddrs *ifaddr, *ifa;
        int family;
        if (getifaddrs(&ifaddr) == -1) {
                return ret;
        }

        for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
                if (ifa->ifa_addr == NULL)
                        continue;

                family = ifa->ifa_addr->sa_family;

                if((family==AF_INET)&&!strcmp(ifa->ifa_name,interface))
                {
                        char address[20]={0};
                        sockaddr_in * addr_in=(sockaddr_in *)ifa->ifa_addr;
                        ret=inet_ntop(family,&addr_in->sin_addr,address,20);
                        break;
                }
        }

        freeifaddrs(ifaddr);

        return ret;
}

void CheckLogOvertime(evutil_socket_t fd, short what,void * arg)
{
       // LOG(DEBUG)<<"overtime check starts";
        struct ::tm currtimeinfo,lasttimeinfo;
        time_t currtime =time(0);
        ::localtime_r(&currtime,&currtimeinfo);

        ::localtime_r(&g_logcreatedtime,&lasttimeinfo);

        if((currtimeinfo.tm_year!=lasttimeinfo.tm_year)||(currtimeinfo.tm_mon!=lasttimeinfo.tm_mon)||(currtimeinfo.tm_mday!=lasttimeinfo.tm_mday))
        {
                LOG(INFO)<<"logs changes!";
                g_logcreatedtime=currtime;
                el::Configurations conf("./log.conf");
                el::Loggers::reconfigureLogger("default",conf);

        }


}

void ShrinkThreadPool(evutil_socket_t fd, short what,void * arg)
{
        EasyServer * pes=static_cast<EasyServer *>(arg);
        if(pes){
                pes->thread_pool_->ShrinkQueueToFit();
                LOG(DEBUG)<<"Shrink thread pool queue size";
        }

        if(g_mongodb_lib){
                g_mongodb_lib->ShrinkQueueToFit();
                LOG(DEBUG)<<"Shrink mongo queue size";
        }
}


int main(int argc, char *argv[])
{
        el::Configurations conf("./log.conf");
        el::Loggers::reconfigureLogger("default",conf);
        g_logcreatedtime=time(0);

        //读取配置
        pt::ptree tree;
        pt::read_xml("./dxp_server.xml", tree);

        // int ter_tcp_port=tree.get("server.tcpterport",7890);
        // int udp_port=tree.get("server.udpport",4001);
        int num_of_threads=tree.get("server.threadnum",10);
        // int timespan=tree.get("server.timespan",2);
        int overtime=tree.get("server.overtime",20);
        int threadpool=tree.get("server.threadpool",12);
        int mongothreads=tree.get("server.mongothreads",12);
        std::string if_name=tree.get("server.ifname","eth0");
        g_redis_server_ip=tree.get("server.redisserver","127.0.0.1");
        g_redis_port=tree.get("server.redisport",6379);
        std::string kafka_broker=tree.get("server.kafkabroker","192.168.216.25,192.168.216.24,192.168.216.25");
        std::string mongodb_server=tree.get("server.mongodbserver","mongodb://localhost:27017/");
        g_mongodb_db=tree.get("server.mongodbdb","mydb");

        //coll
        g_mongocoll_deviceinfo=tree.get("server.mongodbdeviceinfocoll","dxpDeviceInfo");
        g_mongocoll_deviceconf=tree.get("server.mongodbdeviceconfcoll","dxpDeviceConfig");
        g_mongocoll_devicelocation=tree.get("server.mongodbdevicelocationcoll","dxpDeviceLocation");
        g_mongocoll_tripdata=tree.get("server.mongodbtripdatacoll","dxpTripData");
        g_mongocoll_towingdata=tree.get("server.mongodbtowingdatacoll","dxpTowingData");
        g_mongocoll_crashdata=tree.get("server.mongodbcrashdatacoll","dxpCrashData");
        g_mongocoll_firstlocatetime=tree.get("server.mongodbfirstlocatetimecoll","dxpFirstLocateTime");
        //for mxp
        g_mongocoll_mxp=tree.get("server.mongodbmxpcoll","mxpColl");

        //for mxpupdate
        std::string g_mxp_update_b0=tree.get("server.mxpupdateb0","T30_t59_b0.bin");
        std::string g_mxp_update_b1=tree.get("server.mxpupdateb1","T30_t59_b1.bin");

        // g_mongodb_coll=tree.get("server.mongodbcoll","mycoll");
        g_api_server_topic=tree.get("server.apiservertopic","dxp-service-topic");

        g_update_bin_path=tree.get("server.updatebinpath","./");
        g_slice_size=tree.get("server.slicesize",1000);

        g_server_address=GetIpByInterface(if_name.c_str());
        if(g_server_address.empty())
        {
                LOG(ERROR)<<"EasyServer can't get local ip"<<std::endl;
                return -1;
        }

        EasyServer es(num_of_threads,threadpool);
        if(!es.Init()){
                return -1;
        }

        //for overtime check
        es.AddOvertimeListener(5,CheckLogOvertime,NULL);

        //for thread pool queue shrink and mongo queue shrink
        es.AddOvertimeListener(300,ShrinkThreadPool,&es);

        //for tcp
        int tcpportnum=tree.get("server.tcpportnum",2);
        for(int i=0;i<tcpportnum;++i){
                std::string strtcpport=std::string("server.tcpport")+std::to_string(i);
                int tcpport=tree.get(strtcpport,4001+i);
                //add tcp listener
                TcpPacketHandleCb tcphcb(DxpUploadData,tcpport,false,false,GetDxpLen,4,GetResult,DeleteTcpConnInfoFromRedis);

                if(es.AddTcpListener(tcpport,tcphcb)){
                        LOG(INFO)<<"Start Tcp listening on port "<<tcpport;
                        g_config_info+="Tcp listen on port "+std::to_string(tcpport)+"\n";
                }
                else{
                        return -1;
                }
        }

        //for tcp sms
        int tcpsmsportnum=tree.get("server.tcpsmsportnum",1);
        for(int i=0;i<tcpsmsportnum;++i){
                std::string strtcpsmsport=std::string("server.tcpsmsport")+std::to_string(i);
                int tcpsmsport=tree.get(strtcpsmsport,4001+i);
                //add tcp listener
                TcpPacketHandleCb tcphcb(HandleSmsMxpData,tcpsmsport);
                if(es.AddTcpListener(tcpsmsport,tcphcb)){
                        LOG(INFO)<<"Start Tcp SMS listening on port "<<tcpsmsport;
                        g_config_info+="Tcp SMS listen on port "+std::to_string(tcpsmsport)+"\n";
                }
                else{
                        return -1;
                }
        }

        //for tcp ter
        int tcpterportnum=tree.get("server.tcpterportnum",1);
        for(int i=0;i<tcpterportnum;++i){
                std::string strtcpterport=std::string("server.tcpterport")+std::to_string(i);
                int tcpterport=tree.get(strtcpterport,4001+i);
                //add tcp listener
                // TcpPacketHandleCb tcphcb(GetDxpSessionInfo,tcpterport);
                // if(es.AddTcpListener(tcpterport,tcphcb)){
                //         LOG(INFO)<<"Start Tcp ter listening on port "<<tcpterport;
                // }
                // else{
                //         return -1;
                // }
                if(es.AddMonitorListener(tcpterport,MonitorServer)){
                        LOG(INFO)<<"Start Tcp monitor listening on port "<<tcpterport;
                        g_config_info+="Tcp ter listen on port "+std::to_string(tcpterport)+"\n";
                }
                else{
                        return -1;
                }
        }

        //add tcp conn factory
        std::shared_ptr<TcpConnFactory> ptrfac(new DxpTcpConnFactory);
        es.SetTcpConnItemFactory(ptrfac);

        //for udp
        InitCompositeIds();

        int udpportnum=tree.get("server.udpportnum",3);
        for(int i=0;i<udpportnum;++i){
                std::string strudpport=std::string("server.udpport")+std::to_string(i);
                int udpport=tree.get(strudpport,4001+i);
                //add udp listener
                UdpPacketHandleCb udpcb(HandleMxpData,udpport,false);
                if(es.AddUdpListener(udpport,udpcb)){
                        LOG(INFO)<<"Start Udp listening on port "<<udpport;
                        g_config_info+="udp listen on port "+std::to_string(udpport)+"\n";
                }
                else{
                        return -1;
                }
        }


        //kafka
        KafkaLib kb;
        g_kafka_lib=&kb;
        if(!kb.InitKafkaLib(kafka_broker.c_str(),g_server_address.c_str()))
        {
                LOG(ERROR)<<"init kafka lib failed";
                return -1;
        }
        //set kafka consume callback
        // kb.SetConsumeCB(KafkaConsume,&es);


        //start kafka consume thread
        if(!kb.StartConsumeMessage(g_server_address+"dm",KafkaConsumeForDm,&es)){
                LOG(ERROR)<<"start kafka consume thread failed!";
                return -1;
        }

        //start kafka consume thread
        if(!kb.StartConsumeMessage(g_server_address,KafkaConsume,&es)){
                LOG(ERROR)<<"start kafka consume thread failed!";
                return -1;
        }
        //mongo db
        MongodbLib ml;
        g_mongodb_lib=&ml;
        if(!ml.init(mongodb_server))
        {
                LOG(ERROR)<<"init mongodb lib failed";
                return -1;
        }

        //start mongo consume thread
        if(!ml.StartConsumeMessage(&ml,mongothreads)){
                LOG(ERROR)<<"start mongo consume thread failed!";
                return -1;
        }
        //mmap update bin for mxp
        // {
        //         MmapUpdatePacket(g_update_bin_path+g_mxp_update_b0,512,g_4t48b0,true);
        //         if(!g_4t48b0.addr){
        //                 LOG(ERROR)<<"Mmap failed!"<<g_mxp_update_b0;
        //                 return -1;
        //         }
        //         else{
        //                 LOG(INFO)<<"Mmap SUCCESS!"<<g_mxp_update_b0;
        //         }

        //         MmapUpdatePacket(g_update_bin_path+g_mxp_update_b1,512,g_4t48b1,true);
        //         if(!g_4t48b1.addr){
        //                 LOG(INFO)<<"Mmap failed!"<<g_mxp_update_b1;
        //                 return -1;
        //         }
        //         else{
        //                 LOG(INFO)<<"Mmap SUCCESS!"<<g_mxp_update_b1;
        //         }

        // }
        // if(!mxpdata||!errormxpdata)
        //         return -1;

        LOG(INFO)<<"Start with "<<num_of_threads<<" threads";
        LOG(INFO)<<"the thread pool has "<<threadpool<<" workers";
        LOG(INFO)<<"mongo threads is "<<mongothreads;
        LOG(INFO)<<"local ip is "<<g_server_address;
        LOG(INFO)<<"redis server is "<<g_redis_server_ip<<" "<<g_redis_port;
        LOG(INFO)<<"mongodb server is "<<mongodb_server;
        LOG(INFO)<<"kafka server is "<<kafka_broker;

        g_config_info+="worker threads num:"+std::to_string(num_of_threads)+"\n";
        g_config_info+="thread pool threads num:"+std::to_string(threadpool)+"\n";
        g_config_info+="mongo threads num:"+std::to_string(threadpool)+"\n";
        g_config_info+="local ip is:"+g_server_address+"\n";
        g_config_info+="redis server is:"+g_redis_server_ip+" "+std::to_string(g_redis_port)+"\n";
        g_config_info+="mongodb server is:"+mongodb_server+"\n";
        g_config_info+="kafka server is:"+kafka_broker+"\n";

        es.Start();
        return 0;
}




