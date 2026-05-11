import {cpus} from "node:os";
import {Worker} from "worker_threads";

export class ThreadPool {
  constructor(workerPath, size = cpus().length) {
    this.workerPath = workerPath;
    this.size = size;

    this.workers = [];
    this.idleWorkers = [];
    this.jobQueue = [];

    for (let i = 0; i < size; i++) {
      this.createWorker();
    }
  }
  createWorker() {
    const worker = new Worker(this.workerPath);
    const workerMetadata = {
      worker,
      busy: false,
      resolve: null,
      reject: null,
    };

    worker.on("message", (result) => {
      workerMetadata.busy = false;
      if (workerMetadata.resolve) {
        workerMetadata.resolve(result);
      }
      // prevent memory leak
      // send worker to idle for sleep
      workerMetadata.resolve = null;
      workerMetadata.reject = null;
      this.idleWorkers.push(workerMetadata);
      // run next job in queue
      this.runNextJob();
    });

    worker.on("error", (err) => {
      //   set worker to free => idle
      workerMetadata.busy = false;
      if (workerMetadata.reject) {
        workerMetadata.reject(err);
      }
      console.error("Worker error: ", err);
      // remove crashed worker
      this.removeWorker(workerMetadata);
      //  repair pool and replace new worker
      this.createWorker();
      //  run next job in queue
      this.runNextJob();
    });

    worker.on("exit", (code) => {
      if (code !== 0) {
        console.log("Worker exited with code: " + code);
        // remove crashed worker
        this.removeWorker(workerMetadata);
        //  repair pool and replace new worker
        this.createWorker();
      }
    });

    this.workers.push(workerMetadata);
    this.idleWorkers.push(workerMetadata);
  }

  removeWorker(workerMetadata) {
    // check by reference of objects but we can use uuid
    this.workers = this.workers.filter((worker) => worker != workerMetadata);
    this.idleWorkers = this.idleWorkers.filter((worker) => worker != workerMetadata);
  }

  runTask(data) {
    // console.log("data in run task: ", data);
    return new Promise((resolve, reject) => {
      this.jobQueue.push({
        data,
        resolve,
        reject,
      });
      this.runNextJob();
    });
  }
  runNextJob() {
    // not exist any job to do
    if (this.jobQueue.length === 0) return;
    // not found any free worker(all of them are busy)
    if (this.idleWorkers.length === 0) return;
    //get first free worker
    const workerMetadata = this.idleWorkers.shift();
    //get first job in queue
    const job = this.jobQueue.shift();
    // console.log("job: ", job);

    // init workerMetadata
    workerMetadata.busy = true;
    workerMetadata.resolve = job.resolve;
    workerMetadata.reject = job.reject;

    // send job to worker
    workerMetadata.worker.postMessage(job.data);
  }
}
