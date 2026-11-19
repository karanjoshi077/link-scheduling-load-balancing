
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <nlohmann/json.hpp>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <random>
#include <thread>
#include <atomic>
#include <vector>
#include <string>
#include <sstream>
#include <cstring>
#include <cstdlib>
using json=nlohmann::json;
struct Config{std::string ip;int port=0,client_threads=1;};
static void die(const std::string&s){std::cerr<<"error: "<<s<<"\n";std::exit(1);}
static bool send_all(int fd,const char*p,size_t n){size_t o=0;while(o<n){ssize_t k=send(fd,p+o,n-o,MSG_NOSIGNAL);if(k<=0)return false;o+=k;}return true;}
static bool send_all(int fd,const std::string&s){return send_all(fd,s.data(),s.size());}
static void load_config(const std::string&path,Config&c){
 std::ifstream f(path);if(!f)die("cannot open config file '"+path+"'");
 json d;try{f>>d;}catch(...){die("malformed JSON");}
 if(!d.contains("server")||!d["server"].is_object())die("missing required field 'server'");
 auto&s=d["server"];
 if(!s.contains("ip")||!s["ip"].is_string())die("invalid field 'server.ip'");
 if(!s.contains("port")||!s["port"].is_number_integer())die("invalid field 'server.port'");
 if(!s.contains("client_threads")||!s["client_threads"].is_number_integer())die("invalid field 'server.client_threads'");
 c.ip=s["ip"];c.port=s["port"];c.client_threads=s["client_threads"];
}
static int connect_server(const Config&c){
 int fd=socket(AF_INET,SOCK_STREAM,0);if(fd<0)return -1;sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(c.port);
 if(inet_pton(AF_INET,c.ip.c_str(),&a.sin_addr)<=0){close(fd);return -1;}
 if(connect(fd,(sockaddr*)&a,sizeof(a))<0){close(fd);return -1;}return fd;
}
static std::string base(const std::string&p){auto x=p.find_last_of("/\\");return x==std::string::npos?p:p.substr(x+1);}
static bool recv_line(int fd,std::string&line,std::string&overflow){
 line.clear();overflow.clear();char b[4096];
 for(;;){ssize_t n=recv(fd,b,sizeof(b),0);if(n<=0)return false;line.append(b,n);auto p=line.find('\n');if(p!=std::string::npos){
    overflow=line.substr(p+1);
    line.resize(p);
    return true;
}if(line.size()>65536)return false;}
}
static bool do_put(const std::string&path,const Config&c,bool silent=false){
 std::ifstream in(path,std::ios::binary|std::ios::ate);if(!in){if(!silent)std::cerr<<"error: cannot open "<<path<<"\n";return false;}
 long long size=in.tellg();in.seekg(0);std::string name=base(path);int fd=connect_server(c);if(fd<0)return false;
 std::string h="PUT "+name+" "+std::to_string(size)+"\n";if(!send_all(fd,h)){close(fd);return false;}
 std::string ack,ackov;if(!recv_line(fd,ack,ackov)||ack.rfind("OK ",0)!=0){if(!silent)std::cerr<<"error: server: "<<ack<<"\n";close(fd);return false;}
 char b[8192];while(in){in.read(b,sizeof(b));std::streamsize n=in.gcount();if(n>0&&!send_all(fd,b,n)){close(fd);return false;}}
 std::string resp,ov;if(!recv_line(fd,resp,ov)){close(fd);return false;}close(fd);
 bool ok=resp.rfind("OK ",0)==0;
 if(ok&&!silent)std::cout<<"Successfully put "<<name<<" ("<<size<<" bytes)\n";
 if(!ok&&!silent)std::cerr<<"error: server: "<<resp<<"\n";return ok;
}
static bool do_get(const std::string&name,const Config&c,bool silent=false){
 int fd=connect_server(c);if(fd<0){if(!silent)std::cerr<<"error: connection failed\n";return false;}
 if(!send_all(fd,"GET "+name+"\n")){close(fd);return false;}
 std::string header,overflow;if(!recv_line(fd,header,overflow)){close(fd);return false;}
 if(header.rfind("OK ",0)!=0){if(!silent)std::cerr<<"error: server: "<<header<<"\n";close(fd);return false;}
 long long total=0;try{total=std::stoll(header.substr(3));}catch(...){close(fd);return false;}
 std::ofstream out(name,std::ios::binary|std::ios::trunc);if(!out){close(fd);return false;}
 long long got=0;if(!overflow.empty()){out.write(overflow.data(),overflow.size());got+=overflow.size();}
 char b[8192];while(got<total){ssize_t n=recv(fd,b,std::min<long long>(sizeof(b),total-got),0);if(n<=0)break;out.write(b,n);got+=n;}
 out.close();close(fd);bool ok=(got==total);
 if(!ok){if(!silent)std::cerr<<"error: short GET "<<got<<"/"<<total<<" bytes\n";return false;}
 if(!silent)std::cout<<"Successfully got "<<name<<" ("<<got<<" bytes)\n";return true;
}
static void do_load(const std::string&dir,int N,const Config&c){
 std::vector<std::string>files;for(auto&e:std::filesystem::directory_iterator(dir))if(e.is_regular_file())files.push_back(e.path().string());
 if(files.empty())die("workload directory contains no regular files");
 std::cout<<"Seeding server with "<<files.size()<<" files...\n";for(auto&f:files)if(!do_put(f,c,true))die("seeding failed for "+f);
 std::atomic<int>next{0};std::vector<std::thread>ts;
 for(int t=0;t<c.client_threads;t++)ts.emplace_back([&]{std::mt19937 rng(std::random_device{}());std::uniform_int_distribution<size_t>fd(0,files.size()-1);std::uniform_int_distribution<int>od(0,1);
   for(;;){int k=next.fetch_add(1);if(k>=N)break;auto&f=files[fd(rng)];if(od(rng)==0)do_get(base(f),c,true);else do_put(f,c,true);}
 });
 for(auto&t:ts)t.join();std::cout<<"Load generation complete.\n";
}
int main(int argc,char**argv){
 if(argc<3)die("usage: ./client <put|get|load> <target> [--config path] [--requests N]");
 std::string op=argv[1],target=argv[2],cfg="config.json";int N=-1;bool reqflag=false;
 for(int i=3;i<argc;i++){std::string a=argv[i];if(a=="--config"){if(i+1>=argc)die("missing value for --config");cfg=argv[++i];}else if(a=="--requests"){if(i+1>=argc)die("missing value for --requests");N=std::stoi(argv[++i]);reqflag=true;}else die("unknown argument '"+a+"'");}
 if((op=="load")!=reqflag)die(op=="load"?"--requests is required for load":"--requests is only valid with load");if(op!="put"&&op!="get"&&op!="load")die("unknown operation");
 Config c;load_config(cfg,c);if(op=="put")return do_put(target,c)?0:1;if(op=="get")return do_get(target,c)?0:1;do_load(target,N,c);return 0;
}
