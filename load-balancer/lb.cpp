// COL724/7524 Assignment 2 - Part B: Load balancer
//
// A TCP load balancer in front of four Part A file servers. It speaks the
// Part A wire protocol on both sides (GET / PUT, one request per connection),
// health-checks the backends with HEALTH probes, and selects a backend with
// one of three algorithms:
//
//   rr        plain round robin (baseline, reads no state)
//   queue     least last-reported scheduler queue depth   (probe-cadence state)
//   inflight  least requests currently in flight at the LB (real-time state)
//
// Build: g++ -std=c++17 -pthread -I./include -o lb lb.cpp

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

using json = nlohmann::json;
using Clock = std::chrono::steady_clock;

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

static volatile sig_atomic_t g_stop = 0;           // SIGINT / SIGTERM
static std::atomic<bool> g_health_stop{false};     // stops probe threads (after drain)

static void on_signal(int) { g_stop = 1; }

[[noreturn]] static void die(const std::string& msg) {
    std::cerr << "error: " << msg << "\n";
    std::exit(1);
}

static std::string iso_now() {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    tm t{};
    gmtime_r(&tv.tv_sec, &t);
    char buf[48];
    std::snprintf(buf, sizeof buf, "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", t.tm_year + 1900,
                  t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, (int)(tv.tv_usec / 1000));
    return buf;
}

static double ms_between(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct BackendCfg {
    std::string ip;
    int port = 0;
    std::string name() const { return ip + ":" + std::to_string(port); }
};

struct LbConfig {
    std::string lb_ip;
    int lb_port = 0;
    int health_interval_ms = 0;
    std::vector<BackendCfg> backends;
};

// Fetch obj[key], insisting it exists and has the right type; the error message
// names the full dotted path of the offending field.
//   type: 'o' object, 'a' array, 's' string, 'i' integer
// (The returned reference points into `obj`, which outlives it; the compiler cannot see that.)
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic ignored "-Wdangling-reference"
#endif
static const json& need(const json& obj, const std::string& key, const std::string& path, char type) {
    if (!obj.is_object() || !obj.contains(key)) die("missing required field '" + path + "'");
    const json& v = obj[key];
    switch (type) {
        case 'o': if (!v.is_object()) die("field '" + path + "' must be an object"); break;
        case 'a': if (!v.is_array()) die("field '" + path + "' must be an array"); break;
        case 's': if (!v.is_string()) die("field '" + path + "' must be a string"); break;
        case 'i': if (!v.is_number_integer()) die("field '" + path + "' must be an integer"); break;
    }
    return v;
}

static int need_port(const json& obj, const std::string& key, const std::string& path) {
    long long p = need(obj, key, path, 'i').get<long long>();
    if (p < 1 || p > 65535) die("field '" + path + "' must be in 1..65535");
    return (int)p;
}

static void load_config(const std::string& path, LbConfig& c) {
    std::ifstream f(path);
    if (!f) die("cannot open config file '" + path + "'");
    json d;
    try {
        f >> d;
    } catch (const std::exception& e) {
        die(std::string("malformed JSON in '") + path + "': " + e.what());
    }
    // Every field of the shared config is required, including the Part A section.
    const json& s = need(d, "server", "server", 'o');
    need(s, "ip", "server.ip", 's');
    need_port(s, "port", "server.port");
    if (need(s, "server_threads", "server.server_threads", 'i').get<long long>() < 1)
        die("field 'server.server_threads' must be positive");
    if (need(s, "client_threads", "server.client_threads", 'i').get<long long>() < 1)
        die("field 'server.client_threads' must be positive");

    const json& lb = need(d, "load_balancer", "load_balancer", 'o');
    c.lb_ip = need(lb, "ip", "load_balancer.ip", 's').get<std::string>();
    c.lb_port = need_port(lb, "port", "load_balancer.port");
    long long hi = need(lb, "health_interval_ms", "load_balancer.health_interval_ms", 'i').get<long long>();
    if (hi < 1) die("field 'load_balancer.health_interval_ms' must be positive");
    c.health_interval_ms = (int)hi;

    const json& bs = need(lb, "backends", "load_balancer.backends", 'a');
    if (bs.size() != 4) die("field 'load_balancer.backends' must contain exactly 4 entries");
    for (size_t i = 0; i < bs.size(); i++) {
        std::string p = "load_balancer.backends[" + std::to_string(i) + "]";
        if (!bs[i].is_object()) die("field '" + p + "' must be an object");
        BackendCfg b;
        b.ip = need(bs[i], "ip", p + ".ip", 's').get<std::string>();
        b.port = need_port(bs[i], "port", p + ".port");
        c.backends.push_back(b);
    }
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

enum class Algo { RR, QUEUE, INFLIGHT };

struct Options {
    std::string config = "config.json";
    std::string health_log = "health_log.csv";
    std::string dispatch_log = "lb_dispatch.csv";
    Algo algo = Algo::RR;
    bool have_algo = false;
    bool retry = false;           // optional: re-dispatch on failure before any response byte
    int io_timeout_ms = 30000;    // per-recv/send stall limit on both sockets
};

static const char* algo_name(Algo a) {
    return a == Algo::RR ? "rr" : a == Algo::QUEUE ? "queue" : "inflight";
}

static void usage() {
    std::cout << "usage: ./lb --algo <rr|queue|inflight> [--config <path>] [--health-log <path>]\n"
                 "            [--dispatch-log <path>] [--retry] [--io-timeout-ms <ms>]\n";
}

static Options parse_args(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        auto val = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) die(std::string("missing value for ") + flag);
            return argv[++i];
        };
        if (a == "--algo") {
            std::string v = val("--algo");
            if (v == "rr") o.algo = Algo::RR;
            else if (v == "queue") o.algo = Algo::QUEUE;
            else if (v == "inflight") o.algo = Algo::INFLIGHT;
            else die("invalid --algo '" + v + "' (expected rr, queue or inflight)");
            o.have_algo = true;
        } else if (a == "--config") o.config = val("--config");
        else if (a == "--health-log") o.health_log = val("--health-log");
        else if (a == "--dispatch-log") o.dispatch_log = val("--dispatch-log");
        else if (a == "--retry") o.retry = true;
        else if (a == "--io-timeout-ms") {
            try { o.io_timeout_ms = std::stoi(val("--io-timeout-ms")); } catch (...) { die("invalid --io-timeout-ms"); }
            if (o.io_timeout_ms < 1) die("--io-timeout-ms must be positive");
        } else if (a == "--help" || a == "-h") { usage(); std::exit(0); }
        else die("unknown argument '" + a + "'");
    }
    if (!o.have_algo) die("--algo is required (rr, queue or inflight)");
    return o;
}

// ---------------------------------------------------------------------------
// Backend state
// ---------------------------------------------------------------------------

struct Backend {
    BackendCfg cfg;
    std::string name;
    // Guarded by g_state_mu:
    bool healthy = false;      // last probe succeeded
    int queue_depth = 0;       // depth reported by the last successful probe
    int inflight = 0;          // requests currently dispatched here
    // Counters:
    std::atomic<long> forwarded{0};   // requests whose header was sent to this backend
    std::atomic<long> failed{0};      // exchanges that broke on the backend side
    std::atomic<bool> probed{false};  // has completed at least one probe
};

static Options g_opt;
static LbConfig g_cfg;
static std::vector<std::unique_ptr<Backend>> g_backends;
static std::mutex g_state_mu;       // health flag, queue depth, inflight, rr cursors
static size_t g_rr_cursor = 0;
static size_t g_tie_cursor = 0;

// Pick a backend and (atomically with the pick) count it as in flight, so that
// concurrent dispatches under `inflight` see each other. Returns -1 if no
// healthy, non-excluded backend exists.
static int select_backend(unsigned excluded) {
    std::lock_guard<std::mutex> g(g_state_mu);
    const size_t n = g_backends.size();
    auto usable = [&](size_t i) { return g_backends[i]->healthy && !(excluded & (1u << i)); };

    int pick = -1;
    if (g_opt.algo == Algo::RR) {
        for (size_t k = 0; k < n; k++) {
            size_t i = (g_rr_cursor + k) % n;
            if (usable(i)) { pick = (int)i; break; }
        }
        if (pick >= 0) g_rr_cursor = ((size_t)pick + 1) % n;
    } else {
        // Minimum of the algorithm's signal. Ties are broken by rotating the
        // starting point, so equal backends share load instead of the lowest
        // index winning every time.
        long best = 0;
        for (size_t k = 0; k < n; k++) {
            size_t i = (g_tie_cursor + k) % n;
            if (!usable(i)) continue;
            long v = (g_opt.algo == Algo::QUEUE) ? g_backends[i]->queue_depth : g_backends[i]->inflight;
            if (pick < 0 || v < best) { pick = (int)i; best = v; }
        }
        if (pick >= 0) g_tie_cursor = ((size_t)pick + 1) % n;
    }
    if (pick >= 0) g_backends[pick]->inflight++;
    return pick;
}

static void release_backend(int i) {
    std::lock_guard<std::mutex> g(g_state_mu);
    g_backends[i]->inflight--;
}

// ---------------------------------------------------------------------------
// Socket helpers
// ---------------------------------------------------------------------------

static void set_timeouts(int fd, int ms) {
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
}

static bool send_all(int fd, const char* p, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t k = send(fd, p + off, n - off, MSG_NOSIGNAL);
        if (k > 0) off += (size_t)k;
        else if (k < 0 && errno == EINTR) continue;
        else return false;
    }
    return true;
}
static bool send_all(int fd, const std::string& s) { return send_all(fd, s.data(), s.size()); }

enum class LineStatus { Ok, Closed, Error, TooLong };

// Read one '\n'-terminated line. `buf` carries bytes already received; anything
// after the newline stays in `buf` for the body reader.
static LineStatus read_line(int fd, std::string& buf, std::string& line) {
    for (;;) {
        size_t nl = buf.find('\n');
        if (nl != std::string::npos) {
            line.assign(buf, 0, nl);
            buf.erase(0, nl + 1);
            return LineStatus::Ok;
        }
        if (buf.size() > 65536) return LineStatus::TooLong;
        char tmp[4096];
        ssize_t n = recv(fd, tmp, sizeof tmp, 0);
        if (n > 0) buf.append(tmp, (size_t)n);
        else if (n == 0) return LineStatus::Closed;
        else if (errno == EINTR) continue;
        else return LineStatus::Error;   // includes receive timeout
    }
}

enum class Relay { Ok, SrcFailed, DstFailed };

// Copy exactly n bytes from src to dst, consuming `pre` (already-received
// bytes) first. Reports which side failed so callers can attribute the blame.
static Relay relay_bytes(int src, int dst, std::string& pre, long long n) {
    while (n > 0 && !pre.empty()) {
        size_t k = (size_t)std::min<long long>((long long)pre.size(), n);
        if (!send_all(dst, pre.data(), k)) return Relay::DstFailed;
        pre.erase(0, k);
        n -= (long long)k;
    }
    std::vector<char> buf(64 * 1024);
    while (n > 0) {
        ssize_t r = recv(src, buf.data(), (size_t)std::min<long long>((long long)buf.size(), n), 0);
        if (r > 0) {
            if (!send_all(dst, buf.data(), (size_t)r)) return Relay::DstFailed;
            n -= r;
        } else if (r < 0 && errno == EINTR) continue;
        else return Relay::SrcFailed;   // EOF before the declared count, error or timeout
    }
    return Relay::Ok;
}

// Connect with an overall deadline. Returns a connected, blocking socket or -1.
static int connect_with_deadline(const BackendCfg& b, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)b.port);
    if (inet_pton(AF_INET, b.ip.c_str(), &a.sin_addr) <= 0) { close(fd); return -1; }

    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    int rc = connect(fd, (sockaddr*)&a, sizeof a);
    if (rc < 0 && errno != EINPROGRESS) { close(fd); return -1; }
    if (rc < 0) {
        pollfd p{fd, POLLOUT, 0};
        int pr;
        do { pr = poll(&p, 1, timeout_ms); } while (pr < 0 && errno == EINTR);
        if (pr <= 0) { close(fd); return -1; }
        int err = 0;
        socklen_t len = sizeof err;
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err != 0) { close(fd); return -1; }
    }
    fcntl(fd, F_SETFL, fl);
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return fd;
}

static bool parse_count(const std::string& s, long long& out) {
    if (s.empty() || s.size() > 15) return false;
    for (char c : s) if (c < '0' || c > '9') return false;
    out = std::stoll(s);
    return true;
}

// "OK <n>" -> n
static bool parse_ok_line(const std::string& line, long long& n) {
    if (line.rfind("OK ", 0) != 0) return false;
    return parse_count(line.substr(3), n);
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

static std::mutex g_health_log_mu;
static FILE* g_health_log = nullptr;
static std::mutex g_dispatch_log_mu;
static FILE* g_dispatch_log = nullptr;

static void log_probe(const Backend& b, bool ok, double rtt_ms, int depth) {
    std::lock_guard<std::mutex> g(g_health_log_mu);
    std::fprintf(g_health_log, "%s,%s,%s,%.3f,%d\n", iso_now().c_str(), b.name.c_str(),
                 ok ? "healthy" : "down", rtt_ms, ok ? depth : -1);
    std::fflush(g_health_log);
}

static void log_dispatch(unsigned long conn, const std::string& op, const std::string& file,
                         const std::string& backend, const char* outcome, long long bytes,
                         double dur_ms) {
    std::lock_guard<std::mutex> g(g_dispatch_log_mu);
    std::fprintf(g_dispatch_log, "%s,%lu,%s,%s,%s,%s,%lld,%.3f\n", iso_now().c_str(), conn,
                 op.c_str(), file.c_str(), backend.c_str(), outcome, bytes, dur_ms);
    std::fflush(g_dispatch_log);
}

// ---------------------------------------------------------------------------
// Health checking
// ---------------------------------------------------------------------------

struct ProbeResult {
    bool ok = false;
    double rtt_ms = 0;
    int depth = 0;
};

// One probe: connect, send HEALTH, read one complete "OK <depth>" line. The whole
// thing must finish within `timeout_ms`; otherwise it fails.
static ProbeResult probe_once(const BackendCfg& b, int timeout_ms) {
    ProbeResult r;
    auto t0 = Clock::now();
    auto deadline = t0 + std::chrono::milliseconds(timeout_ms);
    auto remaining_ms = [&]() {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        return (int)std::max<long long>(0, left);
    };
    auto finish = [&](bool ok, int depth) {
        r.ok = ok;
        r.depth = depth;
        r.rtt_ms = ms_between(t0, Clock::now());
        return r;
    };

    int fd = connect_with_deadline(b, timeout_ms);
    if (fd < 0) return finish(false, 0);
    if (!send_all(fd, "HEALTH\n")) { close(fd); return finish(false, 0); }

    std::string buf;
    for (;;) {
        size_t nl = buf.find('\n');
        if (nl != std::string::npos) {
            long long depth = 0;
            bool good = parse_ok_line(buf.substr(0, nl), depth) && depth <= 1000000000LL;
            close(fd);
            return finish(good, (int)depth);
        }
        if (buf.size() > 256) break;   // not a HEALTH reply
        pollfd p{fd, POLLIN, 0};
        int pr = poll(&p, 1, remaining_ms());
        if (pr < 0 && errno == EINTR) continue;
        if (pr <= 0) break;            // timed out
        char tmp[256];
        ssize_t n = recv(fd, tmp, sizeof tmp, 0);
        if (n > 0) buf.append(tmp, (size_t)n);
        else if (n < 0 && errno == EINTR) continue;
        else break;                    // closed before a full line
    }
    close(fd);
    return finish(false, 0);
}

// One thread per backend, so a slow probe to one never delays the others.
static void health_loop(Backend* b, int interval_ms) {
    auto next = Clock::now();
    while (!g_health_stop) {
        ProbeResult r = probe_once(b->cfg, interval_ms);
        {
            std::lock_guard<std::mutex> g(g_state_mu);
            b->healthy = r.ok;
            if (r.ok) b->queue_depth = r.depth;
        }
        log_probe(*b, r.ok, r.rtt_ms, r.depth);
        b->probed = true;

        next += std::chrono::milliseconds(interval_ms);
        if (next < Clock::now()) next = Clock::now();   // never try to "catch up"
        while (!g_health_stop && Clock::now() < next)
            std::this_thread::sleep_for(std::chrono::milliseconds(
                std::min<long long>(20, std::max<long long>(1, std::chrono::duration_cast<std::chrono::milliseconds>(next - Clock::now()).count()))));
    }
}

// ---------------------------------------------------------------------------
// Request forwarding
// ---------------------------------------------------------------------------

// How a backend exchange ended, from the point of view of what the client saw.
enum class St {
    Done,          // exchange completed (the backend may still have answered ERR)
    PreResponse,   // backend failed before any response byte reached the client
    MidStream,     // backend failed after the client already received response bytes
    ClientGone,    // the client side failed
};

struct Exchange {
    St st = St::PreResponse;
    bool backend_err = false;
    long long bytes = 0;
};

static bool is_err_line(const std::string& l) { return l.rfind("ERR", 0) == 0; }

static Exchange exchange_get(int cfd, int bfd, const std::string& hdr) {
    Exchange ex;
    if (!send_all(bfd, hdr)) return ex;
    std::string bbuf, line;
    if (read_line(bfd, bbuf, line) != LineStatus::Ok) return ex;     // nothing sent to client yet

    if (is_err_line(line)) {
	std::cerr << "[DEBUG] Backend returned: " << line << "\n";
        if (!send_all(cfd, line + "\n")) { ex.st = St::ClientGone; return ex; }
        ex.st = St::Done;
        ex.backend_err = true;
        return ex;
    }
    long long n = 0;
    if (!parse_ok_line(line, n)) return ex;                          // garbage: treat as backend failure
    if (!send_all(cfd, line + "\n")) { ex.st = St::ClientGone; return ex; }
    ex.bytes = n;

    // From here on the client has seen response bytes: no ERR is possible any more.
    switch (relay_bytes(bfd, cfd, bbuf, n)) {
        case Relay::Ok: ex.st = St::Done; break;
        case Relay::SrcFailed: ex.st = St::MidStream; break;
        case Relay::DstFailed: ex.st = St::ClientGone; break;
    }
    return ex;
}

static Exchange exchange_put(int cfd, int bfd, const std::string& hdr, long long size, std::string& cbuf) {
    Exchange ex;
    if (!send_all(bfd, hdr)) return ex;
    std::string bbuf, line;
    if (read_line(bfd, bbuf, line) != LineStatus::Ok) return ex;     // no ack yet: nothing sent to client

    if (is_err_line(line)) {
        if (!send_all(cfd, line + "\n")) { ex.st = St::ClientGone; return ex; }
        ex.st = St::Done;
        ex.backend_err = true;
        return ex;
    }
    long long ignored = 0;
    if (!parse_ok_line(line, ignored)) return ex;
    if (!send_all(cfd, line + "\n")) { ex.st = St::ClientGone; return ex; }   // forward the ack
    ex.bytes = size;

    // Body: client -> backend. Bytes the client sent right behind the header are in cbuf.
    switch (relay_bytes(cfd, bfd, cbuf, size)) {
        case Relay::Ok: break;
        case Relay::SrcFailed: ex.st = St::ClientGone; return ex;
        case Relay::DstFailed: ex.st = St::MidStream; return ex;
    }
    if (read_line(bfd, bbuf, line) != LineStatus::Ok) { ex.st = St::MidStream; return ex; }
    if (!send_all(cfd, line + "\n")) { ex.st = St::ClientGone; return ex; }
    ex.backend_err = is_err_line(line);
    ex.st = St::Done;
    return ex;
}

static std::atomic<long> g_total_dispatched{0};

static void forward_request(int cfd, unsigned long conn, const std::string& op, const std::string& name,
                            long long size, std::string& cbuf) {
    auto t0 = Clock::now();
    const std::string hdr = (op == "GET") ? "GET " + name + "\n"
                                          : "PUT " + name + " " + std::to_string(size) + "\n";
    const int max_attempts = g_opt.retry ? 4 : 1;
    unsigned excluded = 0;
    int attempts = 0;
    std::string backend_label = "-";
    const char* outcome = "ok";
    long long bytes = 0;

    for (;;) {
        int bi = select_backend(excluded);
        if (bi < 0) {
            send_all(cfd, "ERR no healthy backend available\n");
            outcome = "lb_err_no_backend";
            break;
        }
        Backend& b = *g_backends[bi];
        backend_label = b.name;
        attempts++;

        int bfd = connect_with_deadline(b.cfg, 3000);
        if (bfd < 0) {
            release_backend(bi);
            b.failed++;
            excluded |= 1u << bi;
            if (!(g_opt.retry && attempts < max_attempts)) {
                send_all(cfd, "ERR backend unavailable\n");
                outcome = "lb_err_backend_down";
                break;
            }
            continue;
        }
        set_timeouts(bfd, g_opt.io_timeout_ms);
        b.forwarded++;
        g_total_dispatched++;

        Exchange ex = (op == "GET") ? exchange_get(cfd, bfd, hdr) : exchange_put(cfd, bfd, hdr, size, cbuf);
        close(bfd);
        release_backend(bi);
        bytes = ex.bytes;

        if (ex.st == St::PreResponse) {
            b.failed++;
            excluded |= 1u << bi;
            if (g_opt.retry && attempts < max_attempts) continue;   // client has seen nothing: safe to retry
            send_all(cfd, "ERR backend failed\n");
            outcome = "lb_err_backend_failed";
        } else if (ex.st == St::MidStream) {
            b.failed++;
            outcome = "closed_midstream";    // caller closes the client connection
        } else if (ex.st == St::ClientGone) {
            outcome = "client_gone";
        } else {
            outcome = ex.backend_err ? "backend_err" : "ok";
        }
        break;
    }
    log_dispatch(conn, op, name, backend_label, outcome, bytes, ms_between(t0, Clock::now()));
}

// ---------------------------------------------------------------------------
// Per-connection handler
// ---------------------------------------------------------------------------

static std::atomic<long> g_active_requests{0};

static void handle_client(int cfd, unsigned long conn) {
    int one = 1;
    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    set_timeouts(cfd, 5000);   // header must arrive promptly; a silent client cannot pin us

    std::string cbuf, line;
    LineStatus ls = read_line(cfd, cbuf, line);
    if (ls != LineStatus::Ok) {
        if (ls == LineStatus::TooLong) send_all(cfd, "ERR header too long\n");
        else if (ls == LineStatus::Error) send_all(cfd, "ERR header timeout\n");
        else if (!cbuf.empty()) send_all(cfd, "ERR incomplete header\n");
        close(cfd);
        return;
    }
    set_timeouts(cfd, g_opt.io_timeout_ms);

    std::istringstream iss(line);
    std::string op, name, sz, extra;
    iss >> op;

    if (op == "HEALTH" && !(iss >> extra)) {
        // The LB itself has no scheduler queue; report requests currently being forwarded.
        send_all(cfd, "OK " + std::to_string(g_active_requests.load()) + "\n");
    } else if (op == "GET") {
        if (!(iss >> name) || (iss >> extra)) send_all(cfd, "ERR malformed GET request\n");
        else forward_request(cfd, conn, op, name, 0, cbuf);
    } else if (op == "PUT") {
        long long size = 0;
        if (!(iss >> name >> sz) || (iss >> extra)) send_all(cfd, "ERR malformed PUT request\n");
        else if (!parse_count(sz, size)) send_all(cfd, "ERR invalid byte count\n");
        else forward_request(cfd, conn, op, name, size, cbuf);
    } else {
        send_all(cfd, "ERR unknown request\n");
    }
    close(cfd);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

// Deliberately leaked so detached connection threads can never touch a destroyed object at exit.
static std::mutex& g_conn_mu = *new std::mutex;
static std::condition_variable& g_conn_cv = *new std::condition_variable;
static long g_open_conns = 0;

static void spawn_handler(int cfd, unsigned long conn) {
    {
        std::lock_guard<std::mutex> g(g_conn_mu);
        g_open_conns++;
    }
    try {
        std::thread([cfd, conn] {
            g_active_requests++;
            handle_client(cfd, conn);
            g_active_requests--;
            std::lock_guard<std::mutex> g(g_conn_mu);
            g_open_conns--;
            g_conn_cv.notify_all();
        }).detach();
    } catch (const std::exception&) {
        send_all(cfd, "ERR server busy\n");
        close(cfd);
        std::lock_guard<std::mutex> g(g_conn_mu);
        g_open_conns--;
    }
}

int main(int argc, char** argv) {
    g_opt = parse_args(argc, argv);
    load_config(g_opt.config, g_cfg);

    g_health_log = std::fopen(g_opt.health_log.c_str(), "w");
    if (!g_health_log) die("cannot open health log '" + g_opt.health_log + "'");
    std::fprintf(g_health_log, "timestamp,backend,status,rtt_ms,queue_depth\n");
    std::fflush(g_health_log);
    g_dispatch_log = std::fopen(g_opt.dispatch_log.c_str(), "w");
    if (!g_dispatch_log) die("cannot open dispatch log '" + g_opt.dispatch_log + "'");
    std::fprintf(g_dispatch_log, "timestamp,conn_id,op,filename,backend,outcome,bytes,duration_ms\n");
    std::fflush(g_dispatch_log);

    // Signals: no SA_RESTART, so blocking calls return and the loops can notice g_stop.
    struct sigaction sa{};
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    for (auto& bc : g_cfg.backends) {
        auto b = std::make_unique<Backend>();
        b->cfg = bc;
        b->name = bc.name();
        g_backends.push_back(std::move(b));
    }

    // Listening socket.
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) die("socket failed");
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)g_cfg.lb_port);
    if (inet_pton(AF_INET, g_cfg.lb_ip.c_str(), &addr.sin_addr) <= 0) die("invalid load_balancer.ip");
    if (bind(ls, (sockaddr*)&addr, sizeof addr) < 0) die(std::string("bind failed: ") + strerror(errno));
    if (listen(ls, 1024) < 0) die("listen failed");

    // Health threads start first; wait for one round so that selection never runs blind.
    std::vector<std::thread> health;
    for (auto& b : g_backends) health.emplace_back(health_loop, b.get(), g_cfg.health_interval_ms);
    auto first_deadline = Clock::now() + std::chrono::milliseconds(g_cfg.health_interval_ms + 500);
    for (;;) {
        bool all = true;
        for (auto& b : g_backends) all = all && b->probed.load();
        if (all || Clock::now() > first_deadline || g_stop) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    std::cout << "Load balancer (" << algo_name(g_opt.algo) << (g_opt.retry ? ", retry" : "") << ") on "
              << g_cfg.lb_ip << ":" << g_cfg.lb_port << "\n";
    for (auto& b : g_backends) {
        std::lock_guard<std::mutex> g(g_state_mu);
        std::cout << "  backend " << b->name << (b->healthy ? " healthy (depth " + std::to_string(b->queue_depth) + ")" : " DOWN") << "\n";
    }
    std::cout.flush();

    // Accept loop.
    unsigned long next_conn = 0;
    while (!g_stop) {
        pollfd p{ls, POLLIN, 0};
        int pr = poll(&p, 1, 100);
        if (pr <= 0) continue;
        int cfd = accept(ls, nullptr, nullptr);
        if (cfd < 0) continue;
        spawn_handler(cfd, ++next_conn);
    }

    // Shutdown: connections the kernel already accepted must still be answered.
    fcntl(ls, F_SETFL, fcntl(ls, F_GETFL, 0) | O_NONBLOCK);
    for (;;) {
        int cfd = accept(ls, nullptr, nullptr);
        if (cfd < 0) break;
        spawn_handler(cfd, ++next_conn);
    }
    close(ls);

    {   // Let exchanges in flight complete.
        std::unique_lock<std::mutex> l(g_conn_mu);
        g_conn_cv.wait(l, [] { return g_open_conns == 0; });
    }
    g_health_stop = true;
    for (auto& t : health) t.join();

    // Report.
    long total = 0;
    for (auto& b : g_backends) total += b->forwarded.load();
    std::cout << "\nLoad balancer summary (" << algo_name(g_opt.algo) << ")\n";
    std::cout << "backend           forwarded  fraction  failed\n";
    for (auto& b : g_backends) {
        char row[160];
        std::snprintf(row, sizeof row, "%-17s %9ld  %7.1f%%  %6ld\n", b->name.c_str(), b->forwarded.load(),
                      total ? 100.0 * (double)b->forwarded.load() / (double)total : 0.0, b->failed.load());
        std::cout << row;
    }
    std::cout << "total forwarded: " << total << "\n";
    std::cout << "health log: " << g_opt.health_log << "  dispatch log: " << g_opt.dispatch_log << "\n";
    std::fclose(g_health_log);
    std::fclose(g_dispatch_log);
    return 0;
}
