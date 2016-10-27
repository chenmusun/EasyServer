/*
 * easy_server.h
 *
 *  Created on: 2016年9月28日
 *      Author: mason chen
 */
#ifndef EASY_SERVER_H_
#define EASY_SERVER_H_

#include <unordered_map>
#include<memory>
#include<mutex>
#include<thread>
#include<vector>
#include"common_structs.h"
#include"ThreadPool.h"

class WorkerThread;
class EasyServer;

typedef void (*overtime_cb)(evutil_socket_t fd, short what,void * arg);
typedef unsigned int (*tcppacketlen_cb)(unsigned char * data,int len);
typedef void (*tcppackethandle_cb)(EasyServer * server,int threadindex,const std::string& sessionid,unsigned char * data,int len,bool runinthreadpool);
typedef void (*udppackethandle_cb)(unsigned char * data,int len);
typedef	void (*tcppacketsendresult_cb)(void  * data,int len,const std::string& sessionid,void * arg,int arglen,bool isok);
typedef void (*tcpconnclose_cb)(TcpConnItem * tci);



struct UdpPacketHandleCb{
	UdpPacketHandleCb(udppackethandle_cb c,int p){
		cb=c;
		port=p;
	}
	void operator()(unsigned char * data,int len)
		{
			cb(data,len);
			//回收内存
			nedalloc::nedfree(data);
			LOG(DEBUG)<<"Freed "<<len<<" bytes data for udp";
		}
	udppackethandle_cb cb;
	int port;
};

struct TcpPacketHandleCb{
	TcpPacketHandleCb(tcppackethandle_cb hc,int p,bool tph=false,bool ar=true,tcppacketlen_cb lcb=NULL,int l=-1){
		handlecb=hc;
		lencb=lcb;
		port=p;
		threadpoolhandle=tph;
		len=l;
		autorelease=ar;
	}
	void operator()(EasyServer * server,int threadindex,const std::string& sessionid,unsigned char * data,int len)
		{
			handlecb(server,threadindex,sessionid,data,len,threadpoolhandle);
			//回收内存
			if(autorelease){
				nedalloc::nedfree(data);
				LOG(DEBUG)<<"Freed "<<len<<" bytes data from session "<<sessionid;
			}

		}
	tcppackethandle_cb handlecb;
	tcppacketlen_cb lencb;
	int port;
	int len;
	bool threadpoolhandle;
	bool autorelease;
};

struct TcpListener{
	event_base * tcp_listen_base;
	evconnlistener *tcp_listener;
	std::shared_ptr<std::thread> tcp_listen_thread;
};

struct UdpListener{
	event_base * udp_listen_base;
	struct event  * udp_listen_event;
	int udp_listen_socket;
	std::shared_ptr<std::thread> udp_listen_thread;
};

struct OvertimeListener
{
	struct event  * overtime_listen_event;
	event_base * overtime_listen_base;
	std::shared_ptr<std::thread> overtime_listen_thread;
};


class TcpConnFactory{
public:
	virtual TcpConnItem * CreateTcpConn(int s,int p,int i,const std::string& sid){
		TcpConnItem * ret=new TcpConnItem(s,p,i,sid);
		return ret;
	}
};


class EasyServer{
public:
	EasyServer(int num_of_workers,int num_of_threads_in_threadpool);
	~EasyServer();
	//listener add interface
	bool AddTcpListener(int port);
	bool AddUdpListener(int port);
	bool AddOvertimeListener(int tm,overtime_cb cb,void * arg);

	//create selfdefined tcp connection object
	void SetTcpConnItemFactory(std::shared_ptr<TcpConnFactory> f){
		factory_=f;
	}

	//start server
	void Start();

	/*send data to tcp connection*/
	void SendDataToTcpConnection(int threadindex,const std::string& sessionid,void * data,unsigned int len,bool runinthreadpool,void *arg=NULL,int arglen=0);

	/*close tcp connection*/
	void CloseTcpConnection(int threadindex,const std::string& sessionid,bool runinthreadpool);

	//add callback for handling tcp packet
	void AddTcpPacketHandleCb(TcpPacketHandleCb& hcb){
		vec_tcppackethandlecbs_.push_back(hcb);
	}

	//add callback for handling udp packet
	void AddUdpPacketHandleCb(UdpPacketHandleCb& udpcb){
		vec_udppackethandlecbs_.push_back(udpcb);
	}

	//set tcp packet sending result callback
	void SetTcpPacketSendResult_cb(tcppacketsendresult_cb cb){
		getresultcb_=cb;
	}

	//set tcp conn close callback
	void SetTcpConnClose_cb(tcpconnclose_cb cb){
		tcpclosecb_=cb;
	}

	//get tcp connection by sessionid
	std::shared_ptr<TcpConnItem> GetTcpConnection(int threadindex,const std::string& sessionid);

	//judge tcp connection exists
	bool IsTcpConnectionExist(int threadindex,const std::string& sessionid);

	template<class F, class... Args>
	void InvokeFunctionUsingThreadPool(F f, Args... args)
	{
		thread_pool_->enqueue(f,args...);
	}

	static void FreeResourceByHand(unsigned char * data,int len){
		nedalloc::nedfree(data);
		LOG(DEBUG)<<"Freed "<<len<<" bytes data by hand";
	}

	//used inside,never mind!!!
	/* Tcp 处理 */
	static void AcceptTcpConn(evconnlistener *, int, sockaddr *, int, void *);
	static void AcceptTcpError(evconnlistener *, void *);
	/* Udp 处理 */
	static void AcceptUdpConn(evutil_socket_t fd, short what, void * arg);

	int GetIndexOfIdleWorker()
	{
		int cur_thread_index=0;
		{
			std::lock_guard<std::mutex>  lock(mutex_thread_index_);
			cur_thread_index = (last_thread_index_ + 1) %num_of_workers_; // 轮循选择工作线程
			last_thread_index_ = cur_thread_index;
		}

		return cur_thread_index;
	}

	std::shared_ptr<WorkerThread> GetWorkerByIndex(int index){
		return vec_workers_[index];
	}

	tcppacketsendresult_cb GetTcpPacketSendResult_cb(){
		return getresultcb_;
	}

	tcpconnclose_cb GetTcpConnClose_cb(){
		return tcpclosecb_;
	}
private:
	bool StartTcpListen(TcpListener& tl,int port);
	bool StartUdpListen(UdpListener& ul,int port);
	bool StartOvertimeListen(OvertimeListener& ol,int timespan,overtime_cb cb,void * arg);
	bool CreateAllWorkerThreads();
private:
	int num_of_workers_;
    int num_of_threads_in_threadpool_;
	std::vector<std::shared_ptr<WorkerThread> > vec_workers_;
	std::mutex mutex_thread_index_;
	int last_thread_index_;
	std::vector<TcpListener> vec_tcp_listeners_;
	std::vector<UdpListener> vec_udp_listeners_;
	std::vector<OvertimeListener> vec_overtime_listeners_;
	tcppacketsendresult_cb getresultcb_;
	tcpconnclose_cb tcpclosecb_;
public:
	std::vector<TcpPacketHandleCb> vec_tcppackethandlecbs_;
	std::vector<UdpPacketHandleCb> vec_udppackethandlecbs_;
	std::shared_ptr<TcpConnFactory> factory_;
	std::shared_ptr<ThreadPool> thread_pool_;//线程池
};
#endif


