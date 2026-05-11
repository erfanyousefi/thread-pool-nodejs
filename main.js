import {cpus} from "node:os";
import {ThreadPool} from "../thread-pool.js";

const TOTAL = 100_000_000;
const MAX_THREADS = cpus().length;
const CHUNK = Math.ceil(TOTAL / MAX_THREADS);
const pool = new ThreadPool("./worker.js", MAX_THREADS);
async function start() {
  console.time("worker");
  const tasks = [];
  for (let start = 0; start <= TOTAL; start += CHUNK) {
    const end = Math.min(start + CHUNK - 1, TOTAL);
    tasks.push(pool.runTask({start, end}));
  }
  const result = await Promise.all(tasks);
  console.log("result: ", result);
  console.timeEnd("worker");
}
start();
