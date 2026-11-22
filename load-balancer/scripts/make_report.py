#!/usr/bin/env python3
"""Builds report.pdf (Part B report, max 4 pages) from the figures in results/."""
from reportlab.lib import colors
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import Image, KeepTogether, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

OUT = "report.pdf"
ss = getSampleStyleSheet()
body = ParagraphStyle("body", parent=ss["BodyText"], fontName="Helvetica", fontSize=8.6, leading=11.0,
                      alignment=TA_JUSTIFY, spaceAfter=3.5)
h1 = ParagraphStyle("h1", parent=ss["Heading2"], fontName="Helvetica-Bold", fontSize=10.5, leading=13,
                    spaceBefore=6, spaceAfter=3, textColor=colors.HexColor("#1f3a5f"))
title = ParagraphStyle("title", parent=ss["Title"], fontName="Helvetica-Bold", fontSize=14.5, leading=17, spaceAfter=1)
sub = ParagraphStyle("sub", parent=body, alignment=0, textColor=colors.HexColor("#555555"), spaceAfter=5)
cap = ParagraphStyle("cap", parent=body, fontSize=7.6, leading=9.4, textColor=colors.HexColor("#444444"), alignment=0)
cell = ParagraphStyle("cell", parent=body, fontSize=7.9, leading=9.6, alignment=0, spaceAfter=0)
cellb = ParagraphStyle("cellb", parent=cell, fontName="Helvetica-Bold")
bullet = ParagraphStyle("bullet", parent=body, leftIndent=10, bulletIndent=1, spaceAfter=2)


def P(t, s=body): return Paragraph(t, s)
def B(t): return Paragraph(t, bullet, bulletText="\u2022")


def table(rows, widths, header=True, zebra=True):
    data = [[Paragraph(str(c), cellb if (header and i == 0) else cell) for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    st = [("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
          ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
          ("LINEABOVE", (0, 0), (-1, 0), 0.8, colors.black), ("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.black),
          ("LINEBELOW", (0, -1), (-1, -1), 0.8, colors.black)]
    if header: st.append(("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eef6")))
    if zebra:
        for i in range(2, len(rows), 2): st.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#f6f6f6")))
    t.setStyle(TableStyle(st))
    return t


def img(path, width_cm):
    from PIL import Image as PI
    w, h = PI.open(path).size
    return Image(path, width=width_cm * cm, height=width_cm * cm * h / w)


W = A4[0] - 3.0 * cm
s = []
s.append(P("COL724/7524 Assignment 2 \u2014 Part B: Load Balancer", title))
s.append(P("Karan Joshi \u00b7 2026MCS2654 \u00b7 all numbers below come from the logs in <font face='Courier'>results/</font>", sub))

# ---------------------------------------------------------------- 1
s.append(P("1. Design: what each algorithm reads", h1))
s.append(P("<b>Forwarding.</b> The LB (<font face='Courier'>lb.cpp</font>) runs one thread per client connection, parses the request line, picks a "
           "backend, connects and relays. For GET it reads the backend's <i>entire</i> <font face='Courier'>OK n</font> header line before "
           "forwarding anything, then relays exactly <i>n</i> bytes; for PUT it forwards the ack, relays exactly the declared body "
           "(including any bytes that arrived behind the header) and forwards the final line. A backend failure before the first response byte "
           "reached the client yields <font face='Courier'>ERR &lt;reason&gt;</font>; after that the client connection is closed (B6). Both sockets "
           "have stall timeouts, so no exchange can hang. The unmodified Part A client works against the LB with only a changed config (B2)."))
s.append(P("<b>Health checking.</b> One thread per backend (so a slow probe never delays the others) probes on a fixed cadence of "
           "<font face='Courier'>health_interval_ms</font> = 1000 ms. A probe is connect + <font face='Courier'>HEALTH\\n</font> + one complete "
           "<font face='Courier'>OK depth</font> line within one interval; any failure marks the backend down, the next success returns it to "
           "service. Every probe is appended to the health log. There is no passive marking from the data path, so detection time is purely "
           "the probe's."))
s.append(table([
    ["<b>--algo</b>", "<b>State read</b>", "<b>Class (B12)</b>", "<b>Refreshed</b>"],
    ["rr", "none (baseline)", "\u2014", "\u2014"],
    ["queue", "queue depth from the backend's last HEALTH reply", "probe-cadence", "once per health interval (1 s)"],
    ["inflight", "requests currently in flight from this LB to each backend", "real-time, observed at the LB", "on every dispatch and completion"],
], [1.6 * cm, 7.0 * cm, 4.2 * cm, W - 12.8 * cm]))
s.append(Spacer(1, 3))
s.append(P("Both state-based algorithms pick the minimum of their signal among healthy backends and break ties by rotating the starting "
           "point. For <i>inflight</i> the pick and the counter increment are one atomic step, so concurrent dispatches see each other."))

# ---------------------------------------------------------------- 2
s.append(P("2. Setup", h1))
s.append(P("Everything ran on one laptop over loopback (<b>12 logical CPUs</b>, <font face='Courier'>nproc</font> = 12), so the four "
           "backends, the LB and the load client share the machine; absolute latencies include that contention and only the relative picture is meaningful. "
           "Four Part A servers with <b>--sched fcfs</b> (identical for all, B8), sharing one <font face='Courier'>--file</font> directory so that a GET "
           "can find what a PUT stored via another backend. Backend 4 (:9004) has <font face='Courier'>server_threads</font> = 1, backends 1\u20133 have 4. "
           "Load: the Part A <font face='Courier'>client load</font> with 8 client threads, 6000 requests (plus 5 seeding PUTs, excluded), "
           "on the A27 workload (1 KB, 30 KB, 150 KB short-line files and 30 KB, 150 KB files with lines of 4.6\u20138 KB). "
           "Latency is the LB-observed duration of a request. One run per cell."))


# ---------------------------------------------------------------- 3
s.append(P("3. B13 — distribution with one slow backend", h1))
s.append(P(
    "The three algorithms were tested with FCFS backends, backend 4 using one server thread, "
    "and a 1000 ms health-probe interval. Each run recorded 6005 backend dispatches, including "
    "five setup requests; the workload itself contained 6000 requests. All 6000 workload "
    "requests completed successfully according to the load-balancer outcomes."
))
s.append(img("results_new/fig_b13_distribution.png", 10.5))
s.append(table([
    ["Algorithm", "b1", "b2", "b3", "b4 slow", "Median arrival-to-finish", "p99 arrival-to-finish", "Metric-window rate"],
    ["Round Robin", "25.0%", "25.0%", "25.0%", "25.0%", "0.55 ms", "42.19 ms", "281.82/s"],
    ["Queue", "27.2%", "27.2%", "29.1%", "16.5%", "0.51 ms", "42.15 ms", "279.05/s"],
    ["In-flight", "24.9%", "25.4%", "26.3%", "23.3%", "0.44 ms", "42.13 ms", "264.33/s"],
], [2.4*cm, 1.15*cm, 1.15*cm, 1.15*cm, 1.5*cm, 3.0*cm, 2.7*cm, W-13.05*cm]))
s.append(Spacer(1, 3))
s.append(P(
    "The latency figures above are calculated from the backend metrics CSVs as finish minus arrival; "
    "the rate is the number of metric rows divided by the first-arrival-to-last-finish window. "
    "These are backend-metric-window measurements, not an independent end-to-end client throughput benchmark. "
    "The five setup rows are included in these calculations."
))
s.append(P(
    "<b>Interpretation.</b> Round Robin distributes requests almost equally but does not account for "
    "backend speed. In this run, queue-based dispatch assigned the slow backend a smaller share (16.5%), "
    "while in-flight dispatch assigned it 23.3%. In-flight had the lowest median arrival-to-finish value "
    "in the backend metrics, but its metric-window rate was also the lowest. The p99 values were nearly "
    "identical. These single-run results do not establish a universally fastest algorithm."
))
s.append(img("results_new/fig_b13_between_probes.png", 10.2))
s.append(P(
    "The queue-depth comparison shows why probe-based decisions can be uneven: the scheduler uses a "
    "snapshot from the last health probe, not a continuously refreshed queue length. A depth of zero "
    "can leave the slow backend tied with other backends until the next probe; a positive depth can "
    "temporarily push it out of selection. The observed pattern is specific to this workload and run."
))

# ---------------------------------------------------------------- 4
s.append(P("4. B14 — backend failure and recovery", h1))
s.append(P(
    "Backend 2 (:9002) was killed after load had started and restarted six seconds later. "
    "Health probes ran at a 1000 ms interval. Detection is measured from the kill to the first "
    "probe reporting the backend down. Client-visible failed requests are distinguished from failed "
    "backend attempts, because retry can fail on one backend and still complete the client request."
))
s.append(table([
    ["Run", "Kill to down probe", "Client-visible failed requests", "Failed attempts at backend 2", "Restart to healthy probe", "Restart to first completed request"],
    ["Round Robin", "474 ms", "49", "49", "471 ms", "503 ms"],
    ["Queue", "473 ms", "45", "45", "470 ms", "477 ms"],
    ["In-flight", "471 ms", "4232", "4232", "469 ms", "480 ms"],
    ["In-flight + retry", "455 ms", "0", "110", "451 ms", "483 ms"],
], [2.2*cm, 2.1*cm, 2.7*cm, 2.4*cm, 2.5*cm, W-11.9*cm]))
s.append(Spacer(1, 3))
s.append(img("results_new/fig_b14_timeline.png", 10.3))
s.append(P(
    "<b>Detection and recovery.</b> The down probe arrived about 455–474 ms after the kill, "
    "roughly half a probe interval in these runs. After restart, the first healthy probe arrived "
    "451–471 ms later. The first completed request followed 477–503 ms after restart. These are "
    "single-run observations, not guarantees for all failure timings."
))
s.append(P(
    "<b>Failure behaviour.</b> Before the health probe marks a dead backend down, requests can still "
    "be sent to it. Round Robin and queue recorded 49 and 45 client-visible failures respectively. "
    "In-flight dispatch recorded 4232 failures: a failed connection releases its in-flight count, "
    "so the dead backend can appear to have the lowest load and attract further requests until health "
    "checking intervenes."
))
s.append(P(
    "<b>Retry result.</b> With retry enabled, the LB recorded 110 failed attempts to backend 2 but "
    "zero client-visible failed requests in this run. These are not contradictory: failed attempts "
    "were retried successfully on another backend. Retry can mask failures that happen before any "
    "response is sent to the client; it cannot generally repair a response that has already been "
    "partially transmitted."
))

# ---------------------------------------------------------------- 5
s.append(P("5. Trade-offs, limitations, and conclusion", h1))
s.append(B(
    "<b>Round Robin:</b> simple and evenly distributes requests, but treats fast and slow backends alike."
))
s.append(B(
    "<b>Queue-based:</b> can reduce traffic to a busy backend, but acts on periodically sampled queue "
    "depth. A stale sample can cause temporary over-selection or under-selection."
))
s.append(B(
    "<b>In-flight:</b> reacts to requests dispatched by this LB and their completions, but is not by "
    "itself a failure detector. A backend that fails quickly can have its in-flight count drop and "
    "become an attractor. Pairing it with health checks and retry substantially improved client-visible "
    "outcomes in the tested failure scenario."
))
s.append(B(
    "<b>Limitations:</b> all experiments ran on one laptop over loopback, so processes shared CPU and "
    "I/O resources. Each configuration was run once; results may vary with timing and workload. "
    "B13 latency and metric-window rate are calculated from backend CSV timestamps, and should not be "
    "compared directly with client-observed end-to-end latency or presented as a multi-run benchmark."
))
s.append(P(
    "<b>Conclusion.</b> No algorithm dominated every measured B13 metric. Round Robin gave the most "
    "even distribution, queue-based dispatch reduced the slow backend's share in this run, and "
    "in-flight dispatch had the lowest median backend-metric arrival-to-finish time. For B14, the "
    "in-flight signal needed health checking and retry to avoid repeatedly sending client requests "
    "to a failed backend."
))

doc = SimpleDocTemplate(OUT, pagesize=A4, leftMargin=1.5 * cm, rightMargin=1.5 * cm, topMargin=1.3 * cm, bottomMargin=1.3 * cm,
                        title="COL724 Assignment 2 Part B report", author="Karan Joshi 2026MCS2654")
doc.build(s)
print("wrote", OUT)
