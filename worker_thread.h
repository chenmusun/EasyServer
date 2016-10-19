/*
 * worker_thread.h
 *
 *  Created on: 2016年9月29日
 *      Author: mason chen
 */
#ifndef WORKER_THREAD_H_
#define WORKER_THREAD_H_
#include<memory>
#include<mutex>
#include<thread>
#include<queue>
#include<unordered_map>
#include"common_structs.h"
class EasyServer;

class WorkerThread
{
public:
	WorkerThread(EasyServer * es,int i);
	~WorkerThread();
	bool Run();//运行工作线程

	//Handle all kinds of notifications
	static void HandleNotifications(evutil_socket_t fd, short what, void * arg);

	//TCP 处理
	static void HandleTcpConn(WorkerThread* pwt);
	static void TcpConnReadCb(bufferevent * bev,void *ctx);
	static void TcpConnEventCB(bufferevent *bev,short int  events,void * ctx);

	void PushTcpConnIntoQueue(SocketPort& sp){
		std::lock_guard<std::mutex>  lock(mutex_tcp_queue_);
		queue_tcp_conns_.push(sp);
	}

	SocketPort PopTcpConnFromQueue()
	{
		std::lock_guard<std::mutex>  lock(mutex_tcp_queue_);
		SocketPort ret=queue_tcp_conns_.front();
		queue_tcp_conns_.pop();
		return ret;
	}

	void InsertTcpConnItem(std::shared_ptr<TcpConnItem> ptci){
		un_map_tcp_conns_.insert(std::make_pair(ptci->sessionid,ptci));
	}

	std::shared_ptr<TcpConnItem> FindTcpConnItem(const std::string& sessionid)
	{
		auto pos=un_map_tcp_conns_.find(sessionid);
		if(pos!=un_map_tcp_conns_.end())
			return pos->second;
		else {
			return std::shared_ptr<TcpConnItem>();
		}
	}

	bool IsTcpConnItemExist(const std::string& sessionid){
		bool ret=false;
		auto pos=un_map_tcp_conns_.find(sessionid);
		if(pos!=un_map_tcp_conns_.end())
			ret=true;
		return ret;
	}

	void DeleteTcpConnItem(const std::string& sessionid){
		un_map_tcp_conns_.erase(sessionid);
	}


	bool NotifyWorkerThread(const char* pchar)
	{
		std::lock_guard<std::mutex> lock(mutex_notify_send_fd_);
		if(write(notfiy_send_fd_,pchar, 1)!=1)
			return false;
		return true;
	}

	//要保证操作的原子性
	bool PushTcpConnIntoQueueAndSendNotify(SocketPort& sp){
		bool ret=true;
		std::lock_guard<std::mutex>  lock(mutex_push_tcp_conn_notify_);
		PushTcpConnIntoQueue(sp);
		if(!NotifyWorkerThread("t")){
			PopTcpConnFromQueue();
		}

		return ret;
	}

	//下发数据处理
	//TODO

	void PushDataIntoQueue(SessionData& sd){
		std::lock_guard<std::mutex>  lock(mutex_data_queue_);
		queue_datas_.push(sd);
	}

	SessionData PopDataFromQueue()
	{
		std::lock_guard<std::mutex>  lock(mutex_data_queue_);
		SessionData ret=queue_datas_.front();
		queue_datas_.pop();
		return ret;
	}

	bool PushDataIntoQueueAndSendNotify(SessionData& sd){
		bool ret=true;
		std::lock_guard<std::mutex>  lock(mutex_push_data_notify_);
		PushDataIntoQueue(sd);
		if(!NotifyWorkerThread("d")){
			PopDataFromQueue();
			ret=false;
		}

		return ret;
	}

	void SendDataToTcpConnection(void * data,int len,const std::string& sessionid,void *arg,int arglen);

	//kill connection

	void PushKillIntoQueue(SessionKill& sk){
		std::lock_guard<std::mutex>  lock(mutex_kill_queue_);
		queue_kills_.push(sk);
	}

	SessionKill PopKillFromQueue()
	{
		std::lock_guard<std::mutex>  lock(mutex_kill_queue_);
		SessionKill ret=queue_kills_.front();
		queue_kills_.pop();
		return ret;
	}

	bool PushKillIntoQueueAndSendNotify(SessionKill& sk){
		bool ret=true;
		std::lock_guard<std::mutex>  lock(mutex_push_kill_notify_);
		PushKillIntoQueue(sk);
		if(!NotifyWorkerThread("k")){
			PopKillFromQueue();
			ret=false;
		}

		return ret;
	}

	void KillTcpConnection(const std::string& sessionid);


private:
	bool CreateNotifyFds();//创建主线程和工作线程通信管道
	bool InitEventHandler();//初始化事件处理器
private:
	evutil_socket_t  notfiy_recv_fd_;//工作线程接收端
	evutil_socket_t  notfiy_send_fd_;//监听线程发送端
	std::mutex mutex_notify_send_fd_;

	std::shared_ptr<std::thread>   ptr_thread_;
	std::unordered_map<std::string,std::shared_ptr<TcpConnItem> > un_map_tcp_conns_;

	//tcp conn
	std::mutex mutex_push_tcp_conn_notify_;
	std::mutex mutex_tcp_queue_;
	std::queue<SocketPort> queue_tcp_conns_;
	//session data
	std::mutex mutex_push_data_notify_;
	std::mutex mutex_data_queue_;
	std::queue<SessionData> queue_datas_;
	//session kill
	std::mutex mutex_push_kill_notify_;
	std::mutex mutex_kill_queue_;
	std::queue<SessionKill> queue_kills_;


	EasyServer * es_;
private:
	struct event  * pnotify_event_; //主线程通知工作线程连接到来事件
	struct event_base * pthread_event_base_;
public:
	int threadindex_;
};
#endif


