//! Fetch jobs for the remote images a flush's blocks referenced, in the v3 fetch topic's job shape.

use std::collections::{BTreeMap, HashSet};

use metrics::counter;
use posthog_replay_anonymizer::{is_public_host, is_tracking_beacon, try_canonicalize};
use replay_core_v4::keys::batch::{FlushKeys, SessionId};
use replay_core_v4::refs::{image_url_ref, reference_hash};
use replay_core_v4::seal::image_url_key;
use serde_json::{json, Value};

use replay_core_v4::inspect::stats::BlockStats;

use crate::batch::Block;

/// The fetcher's limits on one Kafka record: its job count, and the byte length of its value without key or headers.
const MAX_JOBS_PER_RECORD: usize = 1000;
const MAX_RECORD_BYTES: usize = 512 * 1024;
/// How many redirects a job may follow, as in v3.
const REMAINING_HOPS: u32 = 10;

/// Builds one job per new remote image URL, grouped by registrable domain, the topic's key. Counts each block's URLs
/// in the stats paired with it. `already_sent` names references this process sent today.
pub fn fetch_jobs<'b>(
    blocks: impl IntoIterator<Item = (&'b Block, &'b mut BlockStats)>,
    keys: &FlushKeys,
    key_month: &str,
    now_ms: i64,
    already_sent: &dyn Fn(&str) -> bool,
) -> BTreeMap<String, Vec<Value>> {
    let mut jobs: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    let mut seen = HashSet::new();
    for (block, stats) in blocks {
        let id = SessionId {
            team_id: block.session.team_id,
            session_id: block.session.session_id.clone(),
        };
        let (Some(_), Some(team_key)) = (
            keys.sessions.get(&id),
            keys.team_months.get(&block.session.team_id),
        ) else {
            continue;
        };
        let url_key = image_url_key(team_key);
        for raw in &block.image_urls {
            let Ok(url) = try_canonicalize(raw) else {
                continue;
            };
            let path = url::Url::parse(&url.fetch)
                .map(|u| u.path().to_owned())
                .unwrap_or_default();
            if !is_public_host(&url.host) || is_tracking_beacon(&url.host, &path) {
                continue;
            }
            stats.image_urls += 1;
            let reference = image_url_ref(
                block.session.team_id,
                key_month,
                &reference_hash(&url_key, url.dedup.as_bytes()),
            );
            if already_sent(&reference) || !seen.insert(reference.clone()) {
                continue;
            }
            jobs.entry(url.domain.clone()).or_default().push(json!({
                "sessionId": block.session.session_id,
                "originalRef": reference,
                "currentUrl": url.fetch,
                "remainingHops": REMAINING_HOPS,
                "notBeforeMs": 0,
                "firstSeenAtMs": now_ms,
                "fetchCount": 0,
                "republishCount": 0,
                "lastRepublishReason": null,
            }));
        }
    }
    jobs
}

/// Splits each domain's jobs into records the fetcher accepts. Returns (key, payload, references) per record.
/// The fetcher rejects a whole record whose value exceeds its byte limit, so a job too large for any record is dropped.
pub fn records(jobs: BTreeMap<String, Vec<Value>>) -> Vec<(String, Vec<u8>, Vec<String>)> {
    let empty_bytes = payload(Vec::new()).len();
    let mut records = Vec::new();
    let mut oversized = 0u64;
    for (domain, domain_jobs) in jobs {
        let mut batch: Vec<Value> = Vec::new();
        let mut bytes = empty_bytes;
        for job in domain_jobs {
            let size = job.to_string().len();
            if empty_bytes + size > MAX_RECORD_BYTES {
                oversized += 1;
                continue;
            }
            if !batch.is_empty()
                && (batch.len() >= MAX_JOBS_PER_RECORD || bytes + 1 + size > MAX_RECORD_BYTES)
            {
                records.push(record(&domain, std::mem::take(&mut batch)));
                bytes = empty_bytes;
            }
            bytes += size + usize::from(!batch.is_empty());
            batch.push(job);
        }
        if !batch.is_empty() {
            records.push(record(&domain, batch));
        }
    }
    if oversized > 0 {
        counter!("replay_recorder_v4_dropped_image_fetch_jobs_total", "reason" => "oversized")
            .increment(oversized);
    }
    records
}

fn record(domain: &str, jobs: Vec<Value>) -> (String, Vec<u8>, Vec<String>) {
    let references = jobs
        .iter()
        .filter_map(|j| j["originalRef"].as_str().map(str::to_owned))
        .collect();
    (domain.to_owned(), payload(jobs), references)
}

/// The record value exactly as the fetcher measures it.
fn payload(jobs: Vec<Value>) -> Vec<u8> {
    json!({"v": 2, "jobs": jobs}).to_string().into_bytes()
}

#[cfg(test)]
mod tests {
    use replay_core_v4::keys::batch::SessionKey as Key;

    use super::*;
    use crate::batch::SessionKey;

    #[test]
    fn sends_each_new_public_image_url_once_by_domain() {
        let urls = [
            "https://cdn.example.com/a.png",
            "https://cdn.example.com/a.png",
            "https://img2.cdn.example.com/b.png",
            "https://cdn.example.com/sent.png",
            "http://localhost/private.png",
            "https://bat.bing.com/action/0",
        ];
        let block = Block {
            session: SessionKey {
                team_id: 7,
                session_id: "s".to_owned(),
            },
            messages: Vec::new(),
            stats: BlockStats::default(),
            moments: Vec::new(),
            image_urls: urls.iter().map(|u| (*u).to_owned()).collect(),
        };
        let mut stats = BlockStats::default();
        let mut keys = FlushKeys::default();
        keys.sessions.insert(
            SessionId {
                team_id: 7,
                session_id: "s".to_owned(),
            },
            Key {
                key: [1; 32],
                key_month: "2026-10".to_owned(),
            },
        );
        keys.team_months.insert(7, [2; 32]);
        let sent_hash = reference_hash(
            &image_url_key(&[2; 32]),
            try_canonicalize("https://cdn.example.com/sent.png")
                .unwrap()
                .dedup
                .as_bytes(),
        );
        let sent = image_url_ref(7, "2026-10", &sent_hash);

        let jobs = fetch_jobs(
            [(&block, &mut stats)],
            &keys,
            "2026-10",
            1_000,
            &|reference| reference == sent,
        );
        let domains: Vec<_> = jobs.keys().cloned().collect();
        assert_eq!(domains, ["example.com"]);
        let fetched: Vec<_> = jobs["example.com"]
            .iter()
            .map(|j| j["currentUrl"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            fetched,
            [
                "https://cdn.example.com/a.png",
                "https://img2.cdn.example.com/b.png"
            ]
        );
        let job = &jobs["example.com"][0];
        assert!(job["originalRef"]
            .as_str()
            .unwrap()
            .starts_with("imageurl:v4:7:2026-10:"));
        assert_eq!(
            (job["sessionId"].as_str(), job["remainingHops"].as_u64()),
            (Some("s"), Some(10))
        );
        // Every public URL the block referenced counts, sent before or not.
        assert_eq!(stats.image_urls, 4);
    }

    #[test]
    fn splits_a_domains_jobs_at_the_fetchers_record_limit() {
        let jobs = BTreeMap::from([(
            "example.com".to_owned(),
            (0..2500)
                .map(|i| json!({"originalRef": format!("r{i}")}))
                .collect(),
        )]);
        let sizes: Vec<_> = records(jobs)
            .iter()
            .map(|(_, _, references)| references.len())
            .collect();
        assert_eq!(sizes, [1000, 1000, 500]);
    }

    fn padded(reference: &str, pad: usize) -> Value {
        json!({"originalRef": reference, "pad": "x".repeat(pad)})
    }

    #[test]
    fn fills_a_record_to_the_fetchers_byte_limit_and_no_further() {
        let empty = payload(Vec::new()).len();
        let bare = padded("a", 0).to_string().len();
        // Two jobs and a comma that fill the record value to the exact limit.
        let first = MAX_RECORD_BYTES - empty - 2 * bare - 1 - 10;
        for (extra, expected) in [(0, vec![2]), (1, vec![1, 1])] {
            let jobs = BTreeMap::from([(
                "example.com".to_owned(),
                vec![padded("a", first), padded("b", 10 + extra)],
            )]);
            let records = records(jobs);
            let sizes: Vec<_> = records.iter().map(|(_, _, r)| r.len()).collect();
            assert_eq!(sizes, expected, "extra {extra}");
            assert!(records.iter().all(|(_, p, _)| p.len() <= MAX_RECORD_BYTES));
            if extra == 0 {
                assert_eq!(records[0].1.len(), MAX_RECORD_BYTES);
            }
        }
    }

    #[test]
    fn drops_only_a_job_too_large_for_any_record() {
        let room = MAX_RECORD_BYTES - payload(Vec::new()).len() - padded("a", 0).to_string().len();
        let jobs = BTreeMap::from([(
            "example.com".to_owned(),
            vec![
                padded("fits", 0),
                padded("a", room + 1),
                padded("full", room - 3),
            ],
        )]);
        let records = records(jobs);
        let references: Vec<_> = records.iter().map(|(_, _, r)| r.clone()).collect();
        assert_eq!(
            references,
            [vec!["fits".to_owned()], vec!["full".to_owned()]]
        );
        assert_eq!(records[1].1.len(), MAX_RECORD_BYTES);
    }
}
