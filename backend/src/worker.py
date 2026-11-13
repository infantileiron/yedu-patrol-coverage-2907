import json,logging,time
from pathlib import Path
import redis
from src.config.settings import REDIS_URL,RUN_RECOVER_INTERVAL
from src.services.run import execute,recover_stale
from src.utils.repository import init_db
logging.basicConfig(level=logging.INFO)
def main():
    init_db(); client=redis.Redis.from_url(REDIS_URL)
    recover_stale(); last_recovery=time.time()
    while True:
        Path("/tmp/worker-heartbeat").write_text(str(time.time()))
        try:
            item=client.blpop("coverage-map:jobs", timeout=3)
            if item:
                raw=item[1].decode()
                try: job=json.loads(raw)
                except ValueError: job={"id":raw,"attempt":None}
                execute(job["id"],job.get("attempt"))
            if time.time()-last_recovery>=RUN_RECOVER_INTERVAL:
                recover_stale(); last_recovery=time.time()
        except Exception:
            logging.exception("worker failure");time.sleep(2)
if __name__=="__main__": main()
