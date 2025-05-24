# #!/usr/bin/env python3
# import os
# import requests
# import time
# import json
# from kafka import KafkaProducer

# # ── CONFIGURATION ──────────────────────────────────────────────────────────────
# BASE_URL = "https://api.stackexchange.com/2.3/questions"
# ANSWERS_URL_TPL = "https://api.stackexchange.com/2.3/questions/{id}/answers"
# BOOTSTRAP_SERVERS = "localhost:29092"

# API_KEY = os.getenv("STACK_API_KEY", "rl_8rnDNiX6NkfoTHb3o4SX6XnCC")
# if not API_KEY:
#     print("[WARN] No STACK_API_KEY found in environment; proceeding anonymously")

# producer = KafkaProducer(
#     bootstrap_servers=BOOTSTRAP_SERVERS,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
# )

# WINDOW_SIZE = 60  # seconds per time window
# RATE_LIMIT = 1.5  # pause between windows
# BACKFILL_DAYS = 3  # historical window length


# # ── HELPERS ────────────────────────────────────────────────────────────────────
# def build_params(extra=None):
#     p = {"key": API_KEY, "site": "stackoverflow"}
#     if extra:
#         p.update(extra)
#     return p


# def fetch_questions(from_ts, to_ts):
#     params = build_params(
#         {
#             "order": "desc",
#             "sort": "creation",
#             "pagesize": 90,
#             "fromdate": from_ts,
#             "todate": to_ts,
#         }
#     )

#     for attempt in range(1, 4):
#         try:
#             r = requests.get(BASE_URL, params=params)
#             data = r.json()
#             if r.status_code == 200:
#                 print(f"[DEBUG] quota_remaining = {data.get('quota_remaining')}")
#                 return data.get("items", [])
#             else:
#                 print(
#                     f"[WARN] Q try#{attempt} got {r.status_code}: {data.get('error_message')}"
#                 )
#         except Exception as e:
#             print(f"[ERROR] Q exception on try#{attempt}: {e}")
#         time.sleep(2)
#     return []


# def fetch_answers(question_id):
#     url = f"https://api.stackexchange.com/2.3/questions/{question_id}/answers"
#     all_ans = []
#     page = 1

#     while True:
#         params = {
#             "order": "desc",
#             "sort": "activity",
#             "site": "stackoverflow",
#             "filter": "withbody",
#             "pagesize": 100,
#             "page": page,
#         }
#         if API_KEY:
#             params["key"] = API_KEY

#         r = requests.get(url, params=params)
#         data = r.json()

#         if r.status_code != 200:
#             print(
#                 f"[WARN] Answers QID {question_id} page {page} failed: "
#                 f"{data.get('error_message', r.status_code)}"
#             )
#             break

#         all_ans.extend(data.get("items", []))

#         if not data.get("has_more", False):
#             break

#         page += 1
#         time.sleep(0.1)

#     return all_ans


# def build_question_answer_json(q):
#     out = {
#         "question_id": q["question_id"],
#         "title": q.get("title"),
#         "body": q.get("body"),
#         "creation_date": q.get("creation_date"),
#         "score": q.get("score"),
#         "owner_reputation": q.get("owner", {}).get("reputation", 0),
#         "is_answered": q.get("is_answered", False),  # [ADDED]
#         "tags": q.get("tags", []),  # [ADDED]
#         "answers": [],
#     }

#     answers = fetch_answers(q["question_id"])
#     if answers:
#         accepted = [a for a in answers if a.get("is_accepted")]
#         others = sorted(
#             [a for a in answers if not a.get("is_accepted")],
#             key=lambda x: x.get("score", 0),
#             reverse=True,
#         )
#         chosen = (accepted[:1] + others[:3])[:3]

#         for a in chosen:
#             out["answers"].append(
#                 {
#                     "answer_id": a["answer_id"],
#                     "body": a.get("body"),
#                     "score": a.get("score"),
#                     "is_accepted": a.get("is_accepted", False),
#                     "creation_date": a.get("creation_date"),  # [ADDED]
#                     "owner_reputation": a.get("owner", {}).get("reputation", 0),
#                 }
#             )

#     return out


# # ── WINDOW PROCESSOR ───────────────────────────────────────────────────────────
# def process_window(from_ts, to_ts, mode):
#     print(f"[INFO] ({mode}) {from_ts} → {to_ts}")
#     qs = fetch_questions(from_ts, to_ts)
#     print(f"[INFO] Retrieved {len(qs)} questions.")

#     for q in qs:
#         rec = build_question_answer_json(q)
#         rec["mode"] = mode
#         producer.send("stackoverflow-questions", rec)
#         print(f"[INFO] Sent QID {rec['question_id']} ({mode}) to Kafka")


# # ── MAIN ───────────────────────────────────────────────────────────────────────
# def main():
#     now = int(time.time())
#     start = now - BACKFILL_DAYS * 86400
#     t = start

#     while t < now:
#         process_window(t, t + WINDOW_SIZE, mode="backfill")
#         t += WINDOW_SIZE
#         time.sleep(RATE_LIMIT)

#     print("[INFO] → LIVE mode")
#     while True:
#         end = int(time.time())
#         process_window(end - WINDOW_SIZE, end, mode="live")
#         time.sleep(RATE_LIMIT)


# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
import os
import requests
import time
import json
from kafka import KafkaProducer

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
BASE_URL = "https://api.stackexchange.com/2.3/questions"
ANSWERS_URL_TPL = "https://api.stackexchange.com/2.3/questions/{id}/answers"
BOOTSTRAP_SERVERS = "localhost:29092"

API_KEY = os.getenv("STACK_API_KEY", "rl_8rnDNiX6NkfoTHb3o4SX6XnCC")
if not API_KEY:
    print("[WARN] No STACK_API_KEY found in environment; proceeding anonymously")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

WINDOW_SIZE = 60  # seconds per time window
RATE_LIMIT = 1.5  # pause between windows
BACKFILL_DAYS = 3  # historical window length


# ── HELPERS ────────────────────────────────────────────────────────────────────
def build_params(extra=None):
    p = {"key": API_KEY, "site": "stackoverflow"}
    if extra:
        p.update(extra)
    return p


def fetch_questions(from_ts, to_ts):
    params = build_params(
        {
            "order": "desc",
            "sort": "creation",
            "pagesize": 90,
            "fromdate": from_ts,
            "todate": to_ts,
        }
    )

    for attempt in range(1, 4):
        try:
            r = requests.get(BASE_URL, params=params)
            data = r.json()
            if r.status_code == 200:
                print(f"[DEBUG] quota_remaining = {data.get('quota_remaining')}")
                return data.get("items", [])
            else:
                print(
                    f"[WARN] Q try#{attempt} got {r.status_code}: {data.get('error_message')}"
                )
        except Exception as e:
            print(f"[ERROR] Q exception on try#{attempt}: {e}")
        time.sleep(2)
    return []


def fetch_answers(question_id):
    url = f"https://api.stackexchange.com/2.3/questions/{question_id}/answers"
    all_ans = []
    page = 1

    while True:
        params = {
            "order": "desc",
            "sort": "activity",
            "site": "stackoverflow",
            "filter": "withbody",
            "pagesize": 100,
            "page": page,
        }
        if API_KEY:
            params["key"] = API_KEY

        r = requests.get(url, params=params)
        data = r.json()

        if r.status_code != 200:
            print(
                f"[WARN] Answers QID {question_id} page {page} failed: "
                f"{data.get('error_message', r.status_code)}"
            )
            break

        all_ans.extend(data.get("items", []))

        if not data.get("has_more", False):
            break

        page += 1
        time.sleep(0.1)

    return all_ans


def build_question_answer_json(q):
    out = {
        "question_id": q["question_id"],
        "title": q.get("title"),
        "body": q.get("body"),
        "creation_date": q.get("creation_date"),
        "score": q.get("score"),
        "owner_reputation": q.get("owner", {}).get("reputation", 0),
        "is_answered": q.get("is_answered", False),  # [ADDED]
        "tags": q.get("tags", []),  # [ADDED]
        "answers": [],
    }

    answers = fetch_answers(q["question_id"])
    if answers:
        accepted = [a for a in answers if a.get("is_accepted")]
        others = sorted(
            [a for a in answers if not a.get("is_accepted")],
            key=lambda x: x.get("score", 0),
            reverse=True,
        )
        chosen = (accepted[:1] + others[:3])[:3]

        for a in chosen:
            out["answers"].append(
                {
                    "answer_id": a["answer_id"],
                    "body": a.get("body"),
                    "score": a.get("score"),
                    "is_accepted": a.get("is_accepted", False),
                    "creation_date": a.get("creation_date"),  # [ADDED]
                    "owner_reputation": a.get("owner", {}).get("reputation", 0),
                }
            )

    return out


# ── WINDOW PROCESSOR ───────────────────────────────────────────────────────────
def process_window(from_ts, to_ts, mode):
    print(f"[INFO] ({mode}) {from_ts} → {to_ts}")
    qs = fetch_questions(from_ts, to_ts)
    print(f"[INFO] Retrieved {len(qs)} questions.")

    for q in qs:
        rec = build_question_answer_json(q)
        rec["mode"] = mode
        producer.send("stackoverflow-questions", rec)
        print(f"[INFO] Sent QID {rec['question_id']} ({mode}) to Kafka")


# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    now = int(time.time())
    start = now - BACKFILL_DAYS * 86400
    t = start

    while t < now:
        process_window(t, t + WINDOW_SIZE, mode="backfill")
        t += WINDOW_SIZE
        time.sleep(RATE_LIMIT)

    print("[INFO] → LIVE mode")
    while True:
        end = int(time.time())
        process_window(end - WINDOW_SIZE, end, mode="live")
        time.sleep(RATE_LIMIT)


if __name__ == "__main__":
    main()
