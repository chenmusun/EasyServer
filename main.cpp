#include"easy_server.h"
#include<iostream>
#include<fstream>
#include"mxpdecode.hpp"
using namespace std;


INITIALIZE_EASYLOGGINGPP//初始化日志记录库

void PrintBytes(EasyServer * server,int threadindex,const std::string& sessionid,unsigned char * data,int len)
{
        cout<<"threadindex is "<<threadindex<<endl;
        cout<<"sessionid is "<<sessionid<<endl;
        for(int i=0;i<len;++i){
                cout<<data[i];
        }
}

void PrintUdpBytes(unsigned char * data,int len)
{
        for(int i=0;i<len;++i){
                cout<<data[i];
        }
        cout<<endl;
}

unsigned int GetDxpLen(unsigned char * data,int len)
{
        unsigned int ret=0;
        if(data[0]==0x55){
                uint8_t lsb=data[2];
                uint8_t msb=0;
                if(lsb&0x01)
                        msb=data[3];

                lsb=lsb>>1;
                ret=lsb+msb*128;
                if(len>127)
                        ret+=4;
                else
                        ret+=3;
        }

        return ret;
}


void PrintMxpIdData(MxpIdData* iddata,ofstream& os)
{
        os<<"id : "<<iddata->id<<endl;
        os<<"type : "<<(int)iddata->type<<endl;
        os<<"istest : "<<iddata->istest<<endl;
        os<<"length : "<<iddata->length<<endl;

        // for(int i=0;i<iddata->length;++i){
        //         os<<ConvertNumToHexChar(iddata->data[i]);
        // }

        char buf[40000]={0};
        os<<"iddata : "<<ConvertBytes2HexString(iddata->data,iddata->length,buf);
        os<<std::endl;
        // Convert2HexString();
        if(iddata->next)
                PrintMxpIdData(iddata->next,os);
}

void PrintMxpApplicationData(MxpApplicationData* data,ofstream& os){
        os<<"hasChk: "<<data->hasChk<<endl;
        os<<"hasTS: "<<data->hasTS<<endl;
        os<<"hasIMEI: "<<data->hasIMEI<<endl;
        os<<"version: "<<(int)data->version<<endl;
        if(data->hasTS)
                os<<"TIMESTAMP: "<<data->TIMESTAMP<<endl;
        if(data->hasIMEI)
                os<<"IMEI: "<<(const char *)data->IMEI<<endl;

        // cout<<"datalength: "<<data->length<<endl;
        PrintMxpIdData(data->data,os);
        // PrintBytes(data->data,data->length);
}

std::mutex mutex_mxpdatafile;
// std::mutex mutex_errmxpdatafile;
std::ofstream mxpdata("./mxpdata.txt",ios::app);
std::ofstream errormxpdata("./errormxpdata.txt",ios::app);


void WriteMxpDataToFile(unsigned char * data,int len)
{
        // std::lock_guard<std::mutex>  lock(mutex_errmxpdatafile);
        std::lock_guard<std::mutex>  lock(mutex_mxpdatafile);

        MxpTransportDataWithStatus * mtdst=DecodeMxp(data,len);
        bool decodesuccess=true;
        if(!mtdst){
                errormxpdata<<"Decode Mxp failed"<<endl;
        }
        else{
                if(mtdst->status){
                        errormxpdata<<"Decode Mxp failed and err is "<<ConvertErrNum2Str(mtdst->status)<<std::endl;
                        decodesuccess=false;
                }
                else {
                        MxpApplicationDataWithStatus * madws=mtdst->mxpdata->appdata;
                        if(madws->status){
                                errormxpdata<<"Decode Mxp failed and err is "<<ConvertErrNum2Str(madws->status)<<std::endl;
                                decodesuccess=false;
                        }
                        else{
                                MxpApplicationDataWithStatus * curr=madws;

                                PrintMxpApplicationData(curr->data,mxpdata);

                                while (curr->next) {
                                        curr=curr->next;
                                        if(curr->status){
                                                errormxpdata<<"Decode Mxp failed and err is "<<ConvertErrNum2Str(curr->status)<<std::endl;
                                                decodesuccess=false;
                                        }else{
                                                PrintMxpApplicationData(curr->data,mxpdata);
                                        }

                                }
                        }
                }
        }

        char buf[40000]={0};
        ConvertBytes2HexString(data,len,buf);
        if(!decodesuccess){
                errormxpdata<<"source data : "<<buf;
                errormxpdata<<std::endl;
        }
        else {
                mxpdata<<"source data : "<<buf;
                mxpdata<<std::endl;
        }
}

void GetResult(unsigned char  * data,int len,const std::string& sessionid,void * arg,int arglen,bool isok)
{
        cout<<(const char *)data<<endl;
        cout<<len<<endl;
        cout<<sessionid<<endl;
        cout<<"isok : "<<isok<<endl;
}

struct TcpConnItemSelf:public TcpConnItem
{
        TcpConnItemSelf(int s,int p,int i,const std::string& sid)
                :TcpConnItem(s,p,i,sid){
                isfirstpacket=true;
        };
        bool isfirstpacket;
};

class TcpConnSelfFactory:public TcpConnFactory{
public:
        virtual TcpConnItem * CreateTcpConn(int s,int p,int i,const std::string& sid){
                TcpConnItemSelf * ret=new TcpConnItemSelf(s,p,i,sid);
                return ret;
        }
};


void TestSendBack(EasyServer * server,int threadindex,const std::string& sessionid,unsigned char * data,int len,bool runinthreadpool){
        PrintUdpBytes(data,len);
        std::shared_ptr<TcpConnItem> ptrconn=server->GetTcpConnection(threadindex, sessionid);
        if(ptrconn){
                TcpConnItemSelf* ptrconnself(dynamic_cast<TcpConnItemSelf *>(ptrconn.get()));
                if(ptrconnself){
                        cout<<ptrconnself->isfirstpacket<<endl;
                        ptrconnself->isfirstpacket=!(ptrconnself->isfirstpacket);
                }
        }
        // const char * dataok="ok";
        // sleep(3);

        // server->CloseTcpConnection(threadindex,sessionid,runinthreadpool);
        // server->SendDataToTcpConnection(threadindex,sessionid,(unsigned char *)(const_cast<char *>(dataok)),2,runinthreadpool);
}

int main(int argc, char *argv[])
{
        el::Configurations conf("./log.conf");
        el::Loggers::reconfigureLogger("default", conf);

        if(!mxpdata||!errormxpdata)
                return -1;

        EasyServer es(10,10);
        es.AddTcpListener(4001);
        TcpPacketHandleCb tcphcb(TestSendBack,4001,true);
        es.AddTcpPacketHandleCb(tcphcb);
        es.SetTcpPacketSendResult_cb(GetResult);
        std::shared_ptr<TcpConnFactory> ptrfac(new TcpConnSelfFactory);
        es.SetTcpConnItemFactory(ptrfac);
        // es.AddUdpListener(4001);
        // UdpPacketHandleCb udpcb(WriteMxpDataToFile,4001);
        // es.AddUdpPacketHandleCb(udpcb);

        es.Start();
        return 0;
}




