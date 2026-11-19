
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <csignal>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <nlohmann/json.hpp>
using json=nlohmann::json;
static std::atomic<bool> running{true};
static void sig_handler(int){running=false;}
static uint64_t now_ns(){timespec t{}; clock_gettime(CLOCK_MONOTONIC,&t); return uint64_t(t.tv_sec)*1000000000ULL+t.tv_nsec;}
static bool send_all(int fd,const char* p,size_t n){size_t o=0; while(o<n){ssize_t k=send(fd,p+o,n-o,MSG_NOSIGNAL); if(k<=0)return false; o+=k;} return true;}
static bool send_all(int fd,const std::string&s){return send_all(fd,s.data(),s.size());}
static bool valid_name(const std::string&s){return !s.empty()&&s!="."&&s!=".."&&s.find('/')==std::string::npos;}
struct Config{
 std::string ip; int port=0,server_threads=0,client_threads=0; int quantum=-1,p=1;
 std::string file_dir="workload",metrics_out="metrics.csv",sched="fcfs",config_path="config.json";
};
static void die(const std::string&s){std::cerr<<"error: "<<s<<"\n"; std::exit(1);}
static void load_config(const std::string&path,Config&c){
 std::ifstream f(path); if(!f)die("cannot open config file '"+path+"'");
 json d; try{f>>d;}catch(const std::exception&e){die(std::string("malformed JSON: ")+e.what());}
 auto reqobj=[&](const char* n){if(!d.contains(n))die(std::string("missing required field '")+n+"'"); if(!d[n].is_object())die(std::string("field '")+n+"' must be an object");};
 reqobj("server");
 auto str=[&](const char* p){if(!d["server"].contains(p))die(std::string("missing required field 'server.")+p+"'"); if(!d["server"][p].is_string())die(std::string("field 'server.")+p+"' must be string"); return d["server"][p].get<std::string>();};
 auto integer=[&](const char* p){if(!d["server"].contains(p))die(std::string("missing required field 'server.")+p+"'"); if(!d["server"][p].is_number_integer())die(std::string("field 'server.")+p+"' must be integer"); return d["server"][p].get<int>();};
 c.ip=str("ip"); c.port=integer("port"); c.server_threads=integer("server_threads"); c.client_threads=integer("client_threads");
 if(c.port<1||c.port>65535)die("field 'server.port' out of range"); if(c.server_threads<1)die("field 'server.server_threads' must be positive"); if(c.client_threads<1)die("field 'server.client_threads' must be positive");
}
struct Request{
 int fd=-1,id=0; std::string op,name,overflow,content; long long declared=0,offset=0,deficit=0; bool header_sent=false; int rounds=0; long long forfeited=0; uint64_t arrival=0,start=0;
};
struct Metric{int id;std::string op,name;long long bytes;int rounds;long long forfeited;uint64_t arrival,start,finish;};
class Queue{
 std::list<Request> q; std::mutex m; std::condition_variable cv; std::string policy;
public: Queue(std::string p):policy(std::move(p)){}
 void push(Request r){std::lock_guard<std::mutex>g(m);if(policy=="sjf"){auto it=q.begin();while(it!=q.end()&&it->declared<=r.declared)++it;q.insert(it,std::move(r));}else q.push_back(std::move(r));cv.notify_one();}
 bool pop(Request&r){std::unique_lock<std::mutex>l(m);cv.wait(l,[&]{return !q.empty()||!running.load();});if(q.empty())return false;r=std::move(q.front());q.pop_front();return true;}
 size_t size(){std::lock_guard<std::mutex>g(m);return q.size();}
 void wake(){cv.notify_all();}
};
static std::mutex metrics_m; static std::vector<Metric> metrics;
static std::atomic<long long> a14_fired{0};
static std::mutex file_lock_m; static std::map<std::string,bool> file_busy;
static bool try_mark_busy(const std::string&name){std::lock_guard<std::mutex>g(file_lock_m);if(file_busy[name])return false;file_busy[name]=true;return true;}
static void clear_busy(const std::string&name){std::lock_guard<std::mutex>g(file_lock_m);file_busy[name]=false;}

static void record(const Request&r,uint64_t fin){std::lock_guard<std::mutex>g(metrics_m);metrics.push_back({r.id,r.op,r.name,r.offset,r.rounds,r.forfeited,r.arrival,r.start,fin});}
static void worker(Queue*q,Config c){
 Request r;
 while(q->pop(r)){
   if(r.rounds==0)r.start=now_ns(); r.rounds++;
   if(!valid_name(r.name)){send_all(r.fd,"ERR Invalid filename\n");close(r.fd);continue;}
   long long allowance=(c.sched=="drr"?r.deficit+c.quantum:c.quantum);
   if(c.sched=="fcfs"||c.sched=="sjf")allowance=r.declared-r.offset;
   long long used=0; bool done=false,failed=false;
   if(r.op=="GET"){
     {
       if(!r.header_sent){ if(!send_all(r.fd,"OK "+std::to_string(r.declared)+"\n")) failed=true; else r.header_sent=true; }
       while(!failed && !done){
         std::vector<std::string> group; long long group_bytes=0;
         bool force_end=false;
         long long cursor=r.offset;
         while((int)group.size()<c.p){
           if(cursor>=(long long)r.content.size())break;
           size_t nlpos=r.content.find('\n',(size_t)cursor);
           std::string line; size_t line_end;
           if(nlpos==std::string::npos){line=r.content.substr((size_t)cursor);line_end=r.content.size();}
           else{line=r.content.substr((size_t)cursor,nlpos-(size_t)cursor+1);line_end=nlpos+1;}
           long long L=(long long)line.size();
           if(c.sched=="rr"||c.sched=="drr"){
             long long avail=allowance-used;
             if(L>avail){
               if(c.sched=="rr" && used==0 && L>c.quantum){group.push_back(std::move(line));group_bytes+=L;cursor=(long long)line_end;force_end=true;a14_fired.fetch_add(1);}
               else break;
             }else{group.push_back(std::move(line));group_bytes+=L;cursor=(long long)line_end;}
           }else{group.push_back(std::move(line));group_bytes+=L;cursor=(long long)line_end;}
           if(force_end || (int)group.size()>=c.p)break;
         }
         if(group.empty())break;
         std::string payload; for(auto&s:group)payload+=s;
         if(!send_all(r.fd,payload)){failed=true;break;}
         used+=group_bytes;r.offset=cursor;
         if(r.offset>=r.declared){done=true;break;}
         if(force_end || ((c.sched=="rr"||c.sched=="drr") && used>=allowance))break;
       }
       if(!failed && r.offset>=r.declared)done=true;
       if(!done&&!failed&&(c.sched=="rr"||c.sched=="drr")){
         long long left=allowance-used;
         if(c.sched=="rr"){
            if(left > 0) r.forfeited += left;
        } else {
            r.deficit = left;
        }
       }
     }
   }else if(r.op=="PUT"){
     std::ofstream out(c.file_dir+"/"+r.name,r.offset==0?(std::ios::binary|std::ios::trunc):(std::ios::binary|std::ios::app));
     if(!out){send_all(r.fd,"ERR Cannot open file\n");failed=true;}
     else{
       long long want=std::min(r.declared-r.offset,allowance);
       if(!r.overflow.empty()){long long k=std::min<long long>(want,(long long)r.overflow.size());out.write(r.overflow.data(),k);r.overflow.erase(0,k);want-=k;used+=k;r.offset+=k;}
       char b[8192];
       while(want>0){ssize_t n=recv(r.fd,b,std::min<long long>(sizeof(b),want),0);if(n<=0){failed=true;break;}out.write(b,n);if(!out){failed=true;break;}want-=n;used+=n;r.offset+=n;}
       out.close(); if(!failed&&r.offset>=r.declared)done=true; else if(!failed&&c.sched=="drr")r.deficit=allowance-used;
     }
   }else{send_all(r.fd,"ERR Unknown request\n");failed=true;}
   if(r.op=="PUT" && (done||failed)) clear_busy(r.name);
   if(done&&!failed){
     // Final success for PUT; GET closes after final byte, no extra response.
     if(r.op=="PUT")send_all(r.fd,"OK 0\n");
     uint64_t fin=now_ns();close(r.fd);record(r,fin);
   }else if(failed){close(r.fd);}
   else q->push(std::move(r));
 }
}
struct Pending{int fd;std::string buf;bool parsed=false;Request r;uint64_t born=0;};
static bool set_nonblock(int fd){int f=fcntl(fd,F_GETFL,0);return f>=0&&fcntl(fd,F_SETFL,f|O_NONBLOCK)==0;}
int main(int argc,char**argv){
 signal(SIGINT,sig_handler);signal(SIGTERM,sig_handler);signal(SIGPIPE,SIG_IGN);
 Config c; bool have_sched=false,have_file=false,have_q=false;
 for(int i=1;i<argc;i++){std::string a=argv[i];
  auto need=[&](const char*n)->std::string{if(i+1>=argc)die(std::string("missing value for ")+n);return argv[++i];};
  if(a=="--sched"){c.sched=need("--sched");have_sched=true;}
  else if(a=="--file"){c.file_dir=need("--file");have_file=true;}
  else if(a=="--quantum"){try{c.quantum=std::stoi(need("--quantum"));}catch(...){die("invalid --quantum");}have_q=true;}
  else if(a=="--p"){try{c.p=std::stoi(need("--p"));}catch(...){die("invalid --p");}if(c.p<1)die("--p must be >= 1");}
  else if(a=="--config")c.config_path=need("--config");
  else if(a=="--metrics-out")c.metrics_out=need("--metrics-out");
  else die("unknown argument '"+a+"'");
 }
 if(!have_sched)die("--sched is required"); if(!have_file)die("--file is required");
 if(c.sched!="fcfs"&&c.sched!="sjf"&&c.sched!="rr"&&c.sched!="drr")die("invalid --sched");
 if((c.sched=="rr"||c.sched=="drr")&&!have_q)die("--quantum is required for rr/drr");
 if((c.sched=="fcfs"||c.sched=="sjf")&&have_q)die("--quantum is rejected for fcfs/sjf");
 if((c.sched=="rr"||c.sched=="drr")&&(c.quantum<1))die("--quantum must be positive");
 if(!std::filesystem::is_directory(c.file_dir))die("workload directory does not exist");
 load_config(c.config_path,c);
 // CLI values intentionally survive config loading.
 Queue q(c.sched);std::vector<std::thread> ws;for(int i=0;i<c.server_threads;i++)ws.emplace_back(worker,&q,c);
 int s=socket(AF_INET,SOCK_STREAM,0);if(s<0)die("socket failed");int one=1;setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(one));set_nonblock(s);
 sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(c.port);if(inet_pton(AF_INET,c.ip.c_str(),&a.sin_addr)<=0)die("invalid server.ip");if(bind(s,(sockaddr*)&a,sizeof(a))<0)die(std::string("bind failed: ")+strerror(errno));if(listen(s,1024)<0)die("listen failed");
 std::vector<Pending> pends;int nextid=0;
 std::cout<<"Server running ("<<c.sched<<") on "<<c.ip<<":"<<c.port<<"\n";
 while(running){
   std::vector<pollfd> pf;pf.push_back({s,POLLIN,0});for(auto&p:pends)pf.push_back({p.fd,p.parsed?(short)(POLLHUP|POLLERR):(short)(POLLIN|POLLHUP|POLLERR),0});
   int n=poll(pf.data(),pf.size(),10);if(n<0){if(errno==EINTR)continue;break;}
   size_t pends_before=pends.size();
   if(pf[0].revents&POLLIN){for(;;){sockaddr_in ca{};socklen_t cl=sizeof(ca);int fd=accept(s,(sockaddr*)&ca,&cl);if(fd<0){if(errno==EAGAIN||errno==EWOULDBLOCK)break;break;}set_nonblock(fd);pends.push_back({fd,{}});pends.back().born=now_ns();}}
   for(int i=(int)pends_before-1;i>=0;--i){
     auto &pc=pends[(size_t)i];bool remove=false;
     if(!pc.parsed && (pf[i+1].revents&(POLLIN|POLLHUP|POLLERR))){
       char b[4096];for(;;){ssize_t z=recv(pc.fd,b,sizeof(b),0);if(z>0){pc.buf.append(b,z);if(pc.buf.size()>65536&&pc.buf.find('\n')==std::string::npos){send_all(pc.fd,"ERR Header too long\n");close(pc.fd);remove=true;break;}}else if(z==0){if(!remove){send_all(pc.fd,"ERR Connection closed before header\n");close(pc.fd);remove=true;}break;}else{if(errno==EAGAIN||errno==EWOULDBLOCK)break;close(pc.fd);remove=true;break;}}
       if(!remove){size_t nl=pc.buf.find('\n');if(nl!=std::string::npos){
         std::string h=pc.buf.substr(0,nl);std::string ov=pc.buf.substr(nl+1);std::istringstream iss(h);std::string op,name,extra;iss>>op;
         if(op=="HEALTH" && !(iss>>extra)){send_all(pc.fd,"OK "+std::to_string(q.size())+"\n");close(pc.fd);remove=true;}
         else{
           Request r;r.fd=pc.fd;r.id=++nextid;r.arrival=now_ns();r.overflow=std::move(ov);r.op=op;
           long long declared=-1;
           if(op=="GET"){if(!(iss>>name)|| (iss>>extra)||!valid_name(name)){send_all(pc.fd,"ERR Invalid request\n");close(pc.fd);remove=true;}else{r.name=name;}}
           else if(op=="PUT"){std::string sz;if(!(iss>>name>>sz)||(iss>>extra)||!valid_name(name)){send_all(pc.fd,"ERR Invalid request\n");close(pc.fd);remove=true;}else{try{size_t pos=0;declared=std::stoll(sz,&pos);if(pos!=sz.size()||declared<0)throw std::exception();}catch(...){send_all(pc.fd,"ERR Invalid byte count\n");close(pc.fd);remove=true;}if(!remove){r.name=name;r.declared=declared;}}}
           else{send_all(pc.fd,"ERR Unknown request\n");close(pc.fd);remove=true;}
           if(!remove){pc.r=std::move(r);pc.parsed=true;}
         }
       }}
     }
     if(!pc.parsed && !remove && now_ns()-pc.born>5000000000ULL){send_all(pc.fd,"ERR Header timeout\n");close(pc.fd);remove=true;}
     if(pc.parsed && !remove){
       if(try_mark_busy(pc.r.name)){
         if(pc.r.op=="GET"){
           std::ifstream in(c.file_dir+"/"+pc.r.name,std::ios::binary|std::ios::ate);if(!in){send_all(pc.fd,"ERR File not found\n");close(pc.fd);remove=true;}else{long long declared=in.tellg();in.seekg(0);std::string content((size_t)declared,'\0');if(declared>0 && !in.read(&content[0],declared)){send_all(pc.fd,"ERR Read failed\n");close(pc.fd);remove=true;}else{pc.r.declared=declared;pc.r.content=std::move(content);}}
           clear_busy(pc.r.name);
         }else if(pc.r.op=="PUT"){
           if(!send_all(pc.fd,"OK 0\n")){close(pc.fd);remove=true;clear_busy(pc.r.name);}
         }
         if(!remove){fcntl(pc.fd,F_SETFL,fcntl(pc.fd,F_GETFL,0)&~O_NONBLOCK);pc.r.arrival=now_ns();q.push(std::move(pc.r));remove=true;}
       }
     }
     if(remove){pends.erase(pends.begin()+i);}
   }
 }
 close(s);for(auto&pc:pends){send_all(pc.fd,"ERR Server shutting down\n");close(pc.fd);}q.wake();for(auto&t:ws)t.join();
 std::ofstream out(c.metrics_out);if(!out)die("cannot write metrics output");
 out<<"request_id,op,filename,bytes,rounds,forfeited_bytes,arrival_ns,start_ns,finish_ns\n";for(auto&m:metrics)out<<m.id<<","<<m.op<<","<<m.name<<","<<m.bytes<<","<<m.rounds<<","<<m.forfeited<<","<<m.arrival<<","<<m.start<<","<<m.finish<<"\n";
 std::cout<<"Completed "<<metrics.size()<<" requests. Metrics: "<<c.metrics_out<<"\n";
 if(c.sched=="rr") std::cout<<"A14 escape hatch fired "<<a14_fired.load()<<" times.\n";
}
