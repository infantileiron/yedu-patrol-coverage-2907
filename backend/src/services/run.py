import json,time
from datetime import datetime
from src.models.entities import CoverageRun
from src.utils import repository as repo
from src.utils.identity import new_id, now
from src.services.statistics import analyze
from src.config.settings import TASK_MODE, REDIS_URL, ARTIFACT_DIR, RUN_STALE_SECONDS, RUN_REQUEUE_SECONDS

ACTIVE_STATUSES=("queued","running")
QUEUE_LIMIT=10

def list_runs(): return repo.all_rows("runs")

def _age_seconds(run):
    ts=run.get("updated_at") or run.get("created_at") or ""
    try: return time.time()-datetime.fromisoformat(ts).timestamp()
    except Exception: return float("inf")

def _enqueue(run_id,attempt):
    import redis
    redis.Redis.from_url(REDIS_URL).rpush("coverage-map:jobs",json.dumps({"id":run_id,"attempt":attempt}))

def create(data):
    repo.get("scenarios",data.scenario_id)
    with repo.lock("coverage-map-scenario-"+data.scenario_id):
        runs=repo.all_rows("runs")
        for r in runs:
            if r["scenario_id"]==data.scenario_id and r["status"] in ACTIVE_STATUSES:
                return r
        if sum(r["status"] in ACTIVE_STATUSES for r in runs)>=QUEUE_LIMIT: raise ValueError("运行队列已满，请等待已有任务完成")
        run=repo.put("runs",CoverageRun(new_id("run"),data.scenario_id,created_at=now(),updated_at=now()))
        if TASK_MODE=="inline": return execute(run["id"])
        try:
            _enqueue(run["id"],run["attempt"])
        except Exception:
            run["status"]="failed"; run["error"]="队列不可用，请恢复Redis后重新提交"; run["updated_at"]=now()
            repo.put("runs",run)
            raise ValueError(run["error"])
        return run

def execute(id,attempt=None):
    def claim(run):
        if run["status"]!="queued": return None
        if attempt is not None and run.get("attempt",0)!=attempt: return None
        run["status"]="running"; run["updated_at"]=now()
        return run
    claimed=repo.mutate("runs",id,claim)
    if claimed is None: return repo.get("runs",id)
    attempt=claimed.get("attempt",0)
    try:
        exp=repo.get("scenarios",claimed["scenario_id"])
        dataset=repo.get("datasets",exp["dataset_id"])
        result=analyze(dataset["rows"],[repo.get("candidates",cid) for cid in exp["candidate_ids"]],exp["radius_km"],exp["budget"],exp["max_sites"])
        ARTIFACT_DIR.mkdir(parents=True,exist_ok=True)
        tmp=ARTIFACT_DIR/(id+"."+str(attempt)+".tmp")
        tmp.write_text(json.dumps(result,ensure_ascii=False))
        def finalize(run):
            if run["status"]!="running" or run.get("attempt",0)!=attempt: return None
            run["result"]=result; run["status"]="completed"; run["updated_at"]=now()
            return run
        if repo.mutate("runs",id,finalize) is None:
            tmp.unlink(missing_ok=True)
        else:
            tmp.replace(ARTIFACT_DIR/(id+".json"))
    except Exception as exc:
        def fail(run):
            if run["status"]!="running" or run.get("attempt",0)!=attempt: return None
            run["status"]="failed"; run["error"]=str(exc); run["updated_at"]=now()
            return run
        repo.mutate("runs",id,fail)
    return repo.get("runs",id)

def cancel(id):
    def fn(run):
        if run["status"] not in ACTIVE_STATUSES: raise ValueError("仅排队中或推演中的记录可取消")
        run["status"]="cancelled"; run["updated_at"]=now()
        return run
    return repo.mutate("runs",id,fn)

def recover_stale():
    """中断恢复：超时的 running 重新排队（attempt+1 使旧计算结果失效），丢失的 queued 补投。"""
    recovered=[]
    for snapshot in repo.all_rows("runs"):
        if snapshot["status"] not in ACTIVE_STATUSES: continue
        if snapshot["status"]=="running" and _age_seconds(snapshot)<RUN_STALE_SECONDS: continue
        if snapshot["status"]=="queued" and _age_seconds(snapshot)<RUN_REQUEUE_SECONDS: continue
        def fn(run,snapshot=snapshot):
            if run["status"]!=snapshot["status"] or run.get("updated_at")!=snapshot.get("updated_at"): return None
            if run["status"]=="running":
                run["attempt"]=run.get("attempt",0)+1
                run["recoveries"]=run.get("recoveries",0)+1
                run["status"]="queued"
            run["updated_at"]=now()
            return run
        run=repo.mutate("runs",snapshot["id"],fn)
        if run is None: continue
        try:
            _enqueue(run["id"],run.get("attempt",0))
            recovered.append(run["id"])
        except Exception:
            pass
    return recovered
