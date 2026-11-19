# Name: Karan Joshi | Entry No: 2026MCS2654

# COL724 Assignment 2 - Part A: File Server

## Architecture & Design Choices

The server is designed using an asynchronous, event-driven main thread paired with a pool of worker threads. This ensures high concurrency and separates the lightweight task of parsing from the heavy task of file I/O.

### 1. Event Loop & Parsing
The main thread uses `poll()` with non-blocking sockets to handle all incoming client connections. It reads streams until a newline `\n` is detected, safely buffering partial reads. Once a complete header is parsed, the request is packaged into a `Request` object.

### 2. Concurrency & Per-Filename Reservation
To ensure strict data consistency without introducing a massive bottleneck, we avoided a global file mutex. Instead, we implemented a **Per-Filename Reservation** system:
* The main thread maintains a `file_busy` map (`std::map<std::string, bool>`).
* If a `PUT` or `GET` request targets a filename that is currently busy (being modified), the main thread leaves the request in a `Pending` state rather than queuing it.
* The `poll()` loop briefly sleeps and retries. Once the file is released, the main thread acquires the reservation and admits the request to the worker queue.
* **Result:** `PUT A` and `PUT B` can happen fully concurrently across different worker threads, while `PUT A` and `GET A` strictly block each other, preventing read/write and write/write races (silent data corruption).

### 3. In-Memory GET Snapshots
Instead of streaming `GET` responses chunk-by-chunk directly from the disk, the server takes an atomic, in-memory snapshot of the file (`std::string`) at the moment of admission. 
* **Why:** This ensures that network speeds do not dictate how long a file lock is held. The file is read instantly, the lock is released for pending `PUT`s, and the worker thread handles the slow network transmission in the background.

### 4. Scheduling Policies
* **FCFS / SJF:** Use standard queue pushing (SJF inserts based on `r.declared` bytes).
* **RR / DRR:** Handled inside the worker threads. Workers read/write up to the allowed `quantum` (or `quantum + deficit`), track the remaining offset, and if the request isn't finished, they push it back to the end of the central queue. 
* **A14 Escape Hatch:** If a single line in a `PUT` request exceeds the quantum, the server overrides the quantum and writes the full line to avoid infinite loops, incrementing a tracking counter.
