// Shopping Sessions pipeline in Rust.
// Reads $DATA_DIR, writes Parquet to $OUTPUT_DIR/{user_daily,campaign_daily}. Logs to stderr.

use ahash::{AHashMap, RandomState};
use memmap2::Mmap;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NSHARD: usize = 512;
const CHUNK: usize = 4 << 20;

type B = &'static [u8];

fn log(t0: &Instant, msg: &str) {
    eprintln!("[{:7.3}s] {}", t0.elapsed().as_secs_f64(), msg);
}

fn hasher() -> RandomState {
    RandomState::with_seeds(0x9E37_79B9_7F4A_7C15, 0x2545_F491_4F6C_DD1D, 0x1234_5678_9ABC_DEF0, 0x0F0E_0D0C_0B0A_0908)
}

// ----------------------------------------------------------------------------- files

fn map_file(p: &Path) -> B {
    let f = File::open(p).expect("open");
    let len = f.metadata().expect("meta").len();
    if len == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f).expect("mmap") };
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// Byte ranges (after each file's header line) cut at '\n' boundaries into ~CHUNK pieces.
fn chunks_of(files: &[B]) -> Vec<B> {
    let mut out = vec![];
    for &data in files {
        let start = match memchr(b'\n', data) {
            Some(i) => i + 1,
            None => continue, // header only (or nothing)
        };
        let mut s = start;
        while s < data.len() {
            let mut e = (s + CHUNK).min(data.len());
            if e < data.len() {
                e = match memchr(b'\n', &data[e..]) {
                    Some(i) => e + i + 1,
                    None => data.len(),
                };
            }
            out.push(&data[s..e]);
            s = e;
        }
    }
    out
}

#[inline]
fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    s.iter().position(|&b| b == c)
}

/// Iterate the lines of a chunk ('\n' only ends a line; '\r' is data). Empty lines are skipped (always malformed).
fn for_lines(chunk: B, mut f: impl FnMut(B)) {
    let mut s = 0;
    let n = chunk.len();
    while s < n {
        let e = match memchr(b'\n', &chunk[s..]) {
            Some(i) => s + i,
            None => n,
        };
        if e > s {
            f(&chunk[s..e]);
        }
        s = e + 1;
    }
}

/// Split a line into exactly N fields per the CSV dialect. None = malformed.
/// Each field: Some(value) or None for an empty field.
#[inline]
fn split_fields<const N: usize>(line: B) -> Option<[Option<B>; N]> {
    let mut out: [Option<B>; N] = [None; N];
    let mut i = 0usize;
    let mut s = 0usize;
    let n = line.len();
    loop {
        let e = match memchr(b',', &line[s..]) {
            Some(p) => s + p,
            None => n,
        };
        if i >= N {
            return None;
        }
        let f = &line[s..e];
        let v: B = if f.contains(&b'"') {
            let l = f.len();
            if l < 2 || f[0] != b'"' || f[l - 1] != b'"' || f[1..l - 1].contains(&b'"') {
                return None;
            }
            &f[1..l - 1]
        } else {
            f
        };
        out[i] = if v.is_empty() { None } else { Some(v) };
        i += 1;
        if e == n {
            break;
        }
        s = e + 1;
    }
    if i != N {
        return None;
    }
    Some(out)
}

// ----------------------------------------------------------------------------- values

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[inline]
fn dg(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

#[inline]
fn num(s: &[u8]) -> Option<i64> {
    let mut v = 0i64;
    for &b in s {
        v = v * 10 + dg(b)?;
    }
    Some(v)
}

/// Epoch ms, or None when the value is not a valid F1/F2/F3 timestamp.
/// F3 values too long for i64 are returned as None (they are outside every window anyway).
fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(|b| b.is_ascii_digit()) {
        let mut i = 0;
        while i < s.len() && s[i] == b'0' {
            i += 1;
        }
        let t = &s[i..];
        if t.len() > 18 {
            return None;
        }
        return num(t);
    }
    let l = s.len();
    if l != 20 && l != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = num(&s[0..4])?;
    let mo = num(&s[5..7])?;
    let d = num(&s[8..10])?;
    let h = num(&s[11..13])?;
    let mi = num(&s[14..16])?;
    let se = num(&s[17..19])?;
    if !(1..=12).contains(&mo) || d < 1 || d > days_in_month(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let mut off = 0i64;
    if l == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = num(&s[20..22])?;
        let om = num(&s[23..25])?;
        if oh > 23 || om > 59 {
            return None;
        }
        off = sign * (oh * 60 + om) * 60_000;
    }
    Some((days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se) * 1000 - off)
}

fn parse_price(s: Option<B>) -> Option<u32> {
    let s = s?;
    if !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    let t = &s[i..];
    if t.is_empty() || t.len() > 8 {
        return None;
    }
    let v = num(t)?;
    if (1..=10_000_000).contains(&v) {
        Some(v as u32)
    } else {
        None
    }
}

fn read_manifest(p: &Path) -> (i64, i64) {
    let txt = std::fs::read_to_string(p).expect("manifest");
    let get = |key: &str| -> i64 {
        let i = txt.find(&format!("\"{}\"", key)).expect("key");
        let rest = &txt[i + key.len() + 2..];
        let a = rest.find('"').unwrap() + 1;
        let b = a + rest[a..].find('"').unwrap();
        parse_ts(rest[a..b].as_bytes()).expect("manifest ts")
    };
    (get("window_start"), get("window_end"))
}

/// Simple clean two-column CSV (products, campaigns).
fn read_pairs(p: &Path) -> Vec<(B, B)> {
    let data = map_file(p);
    let mut v = vec![];
    let start = match memchr(b'\n', data) {
        Some(i) => i + 1,
        None => return v,
    };
    for_lines(&data[start..], |line| {
        if let Some([Some(a), Some(b)]) = split_fields::<2>(line) {
            v.push((a, b));
        }
    });
    v
}

// ----------------------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct RawEv {
    h: u64,
    id: B,
    ingest: i64,
    user: B,
    product: B,
    ts: i64,
    price: u32,
    valid: bool,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Ev {
    ph: u64,
    person: B,
    ts: i64,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Ck {
    ph: u64,
    person: B,
    ts: i64,
    cid: B,
    camp: u16,
}

fn transpose<T>(parts: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..NSHARD).map(|_| Vec::with_capacity(parts.len())).collect();
    for p in parts {
        for (s, v) in p.into_iter().enumerate() {
            if !v.is_empty() {
                out[s].push(v);
            }
        }
    }
    out
}

fn buckets<T>() -> Vec<Vec<T>> {
    (0..NSHARD).map(|_| Vec::new()).collect()
}

// ----------------------------------------------------------------------------- main

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let (data_dir, out_dir) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into())),
        )
    };
    let (ws, we) = read_manifest(&data_dir.join("manifest.json"));
    let cws = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let rs = hasher();

    // Dimensions.
    let prod_rows = read_pairs(&data_dir.join("products.csv"));
    let mut cat_names: Vec<B> = prod_rows.iter().map(|r| r.1).collect();
    cat_names.push(b"unknown");
    cat_names.sort();
    cat_names.dedup();
    let cat_ix: AHashMap<B, u16> = cat_names.iter().enumerate().map(|(i, &c)| (c, i as u16)).collect();
    let unknown_cat = cat_ix[&b"unknown"[..]];
    let products: AHashMap<B, u16> = prod_rows.iter().map(|&(p, c)| (p, cat_ix[&c])).collect();
    let camp_rows = read_pairs(&data_dir.join("campaigns.csv"));
    let campaigns: AHashMap<B, u16> = camp_rows.iter().enumerate().map(|(i, &(c, _))| (c, i as u16)).collect();
    let organic = camp_rows.len() as u16;
    let ncamp = camp_rows.len() + 1;

    let map_all = |sub: &str| -> Vec<B> { csv_files(&data_dir.join(sub)).iter().map(|p| map_file(p)).collect() };
    let ev_files = map_all("events");
    let ck_files = map_all("clicks");
    let lk_files = map_all("links");
    log(&t0, "mapped");

    // ---- links (in parallel with event parsing + dedup)
    let ((names_by_id, id_map), ev_shards) = rayon::join(
        || {
            let pairs: Vec<Vec<(B, B)>> = chunks_of(&lk_files)
                .par_iter()
                .map(|&c| {
                    let mut v = vec![];
                    for_lines(c, |line| {
                        if let Some([Some(a), Some(b)]) = split_fields::<2>(line) {
                            v.push((a, b));
                        }
                    });
                    v
                })
                .collect();
            let mut ids: AHashMap<B, u32> = AHashMap::with_capacity(1 << 20);
            let mut names: Vec<B> = vec![];
            let mut parent: Vec<u32> = vec![];
            fn find(p: &mut [u32], mut x: u32) -> u32 {
                let mut r = x;
                while p[r as usize] != r {
                    r = p[r as usize];
                }
                while p[x as usize] != r {
                    let n = p[x as usize];
                    p[x as usize] = r;
                    x = n;
                }
                r
            }
            for v in &pairs {
                for &(a, b) in v {
                    let mut get = |s: B| -> u32 {
                        let n = names.len() as u32;
                        *ids.entry(s).or_insert_with(|| {
                            names.push(s);
                            parent.push(n);
                            n
                        })
                    };
                    let ia = get(a);
                    let ib = get(b);
                    let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
                    if ra != rb {
                        parent[ra as usize] = rb;
                    }
                }
            }
            let n = names.len();
            let mut best: Vec<u32> = (0..n as u32).collect();
            for i in 0..n as u32 {
                let r = find(&mut parent, i) as usize;
                if names[i as usize] < names[best[r] as usize] {
                    best[r] = i;
                }
            }
            let person_of: Vec<B> = (0..n).map(|i| names[best[find(&mut parent, i as u32) as usize] as usize]).collect();
            (person_of, ids)
        },
        || {
            // parse events into shards by event_id hash
            let parts: Vec<Vec<Vec<RawEv>>> = chunks_of(&ev_files)
                .par_iter()
                .map(|&c| {
                    let mut b = buckets::<RawEv>();
                    for_lines(c, |line| {
                        let f = match split_fields::<7>(line) {
                            Some(f) => f,
                            None => return,
                        };
                        let id = match f[0] {
                            Some(x) => x,
                            None => return,
                        };
                        let ingest = match f[3].and_then(parse_ts) {
                            Some(x) => x,
                            None => i64::MAX, // never happens (ingest_ts is always valid F1)
                        };
                        let ts = f[2].and_then(parse_ts);
                        let typ = f[4].unwrap_or(b"");
                        let purchase = typ == b"purchase";
                        let mut valid = f[1].is_some()
                            && f[5].is_some()
                            && (purchase || typ == b"view" || typ == b"cart")
                            && matches!(ts, Some(t) if t >= ws && t < we);
                        let mut price = 0u32;
                        if valid && purchase {
                            match parse_price(f[6]) {
                                Some(p) => price = p,
                                None => valid = false,
                            }
                        }
                        let ts = ts.unwrap_or(0);
                        if valid && ingest - ts > DAY {
                            valid = false;
                        }
                        let h = rs.hash_one(id);
                        b[(h % NSHARD as u64) as usize].push(RawEv {
                            h,
                            id,
                            ingest,
                            user: f[1].unwrap_or(b""),
                            product: f[5].unwrap_or(b""),
                            ts,
                            price,
                            valid,
                            purchase,
                        });
                    });
                    b
                })
                .collect();
            transpose(parts)
        },
    );
    log(&t0, &format!("links ({} ids) + events parsed", names_by_id.len()));

    let person = |u: B| -> B {
        match id_map.get(u) {
            Some(&i) => names_by_id[i as usize],
            None => u,
        }
    };

    // ---- dedup events per shard; emit valid kept events bucketed by person hash
    let ev_parts: Vec<Vec<Vec<Ev>>> = ev_shards
        .into_par_iter()
        .map(|vs| {
            let mut v: Vec<RawEv> = vs.into_iter().flatten().collect();
            v.sort_unstable_by(|a, b| a.h.cmp(&b.h).then_with(|| a.id.cmp(b.id)).then(a.ingest.cmp(&b.ingest)));
            let mut out = buckets::<Ev>();
            let mut i = 0;
            while i < v.len() {
                let r = v[i];
                let mut j = i + 1;
                while j < v.len() && v[j].h == r.h && v[j].id == r.id {
                    j += 1;
                }
                if r.valid {
                    let p = person(r.user);
                    let ph = rs.hash_one(p);
                    out[(ph % NSHARD as u64) as usize].push(Ev {
                        ph,
                        person: p,
                        ts: r.ts,
                        price: r.price,
                        cat: *products.get(r.product).unwrap_or(&unknown_cat),
                        purchase: r.purchase,
                    });
                }
                i = j;
            }
            out
        })
        .collect();
    let ev_by_person = transpose(ev_parts);
    log(&t0, "events deduped");

    // ---- clicks
    let ck_parts: Vec<Vec<Vec<Ck>>> = chunks_of(&ck_files)
        .par_iter()
        .map(|&c| {
            let mut b = buckets::<Ck>();
            for_lines(c, |line| {
                let f = match split_fields::<4>(line) {
                    Some(f) => f,
                    None => return,
                };
                let (cid, user, camp, ts) = match (f[0], f[1], f[2], f[3]) {
                    (Some(a), Some(u), Some(c), Some(t)) => (a, u, c, t),
                    _ => return,
                };
                let camp = match campaigns.get(camp) {
                    Some(&x) => x,
                    None => return,
                };
                let ts = match parse_ts(ts) {
                    Some(t) if t >= cws && t < we => t,
                    _ => return,
                };
                let p = person(user);
                let ph = rs.hash_one(p);
                b[(ph % NSHARD as u64) as usize].push(Ck { ph, person: p, ts, cid, camp });
            });
            b
        })
        .collect();
    let ck_by_person = transpose(ck_parts);
    log(&t0, "clicks parsed");

    // ---- per person shard: sessions, credit, assists
    struct UserRow {
        person: B,
        day: i32,
        sessions: i64,
        events: i64,
        revenue: i64,
        top: Option<u16>,
    }
    let ncat = cat_names.len();
    let results: Vec<(Vec<UserRow>, Vec<[i64; 4]>)> = ev_by_person
        .into_par_iter()
        .zip(ck_by_person.into_par_iter())
        .map(|(evs, cks)| {
            let mut ev: Vec<Ev> = evs.into_iter().flatten().collect();
            let mut ck: Vec<Ck> = cks.into_iter().flatten().collect();
            ev.sort_unstable_by(|a, b| a.ph.cmp(&b.ph).then_with(|| a.person.cmp(b.person)).then(a.ts.cmp(&b.ts)));
            ck.sort_unstable_by(|a, b| {
                a.ph.cmp(&b.ph).then_with(|| a.person.cmp(b.person)).then(a.ts.cmp(&b.ts)).then_with(|| a.cid.cmp(b.cid))
            });
            ck.dedup_by(|a, b| a.ph == b.ph && a.person == b.person && a.ts == b.ts && a.cid == b.cid);
            let mut rows: Vec<UserRow> = vec![];
            let mut camp_acc: Vec<[i64; 4]> = vec![[0; 4]; ncamp * ndays];
            let mut cat_sum = vec![0i64; ncat];
            let mut cat_touched: Vec<u16> = vec![];
            let mut cnt = vec![0u32; ncamp];
            let mut active: Vec<u16> = vec![];
            let mut pos = vec![u32::MAX; ncamp];

            let mut ci = 0usize;
            let mut i = 0usize;
            while i < ev.len() {
                let (ph, p) = (ev[i].ph, ev[i].person);
                let mut j = i + 1;
                while j < ev.len() && ev[j].ph == ph && ev[j].person == p {
                    j += 1;
                }
                // this person's clicks
                while ci < ck.len() && (ck[ci].ph, ck[ci].person) < (ph, p) {
                    ci += 1;
                }
                let mut cj = ci;
                while cj < ck.len() && ck[cj].ph == ph && ck[cj].person == p {
                    cj += 1;
                }
                let pc = &ck[ci..cj];
                let pe = &ev[i..j];
                // sliding window state for assists
                let (mut lo, mut hi) = (0usize, 0usize);
                // current user-day accumulation
                let mut cur_day = i32::MIN;
                let mut row = UserRow { person: p, day: 0, sessions: 0, events: 0, revenue: 0, top: None };
                let flush = |row: &mut UserRow, cat_sum: &mut Vec<i64>, cat_touched: &mut Vec<u16>, rows: &mut Vec<UserRow>| {
                    let mut best: Option<(i64, u16)> = None;
                    for &c in cat_touched.iter() {
                        let s = cat_sum[c as usize];
                        // categories are indexed in byte order, so a smaller index wins ties
                        match best {
                            None => best = Some((s, c)),
                            Some((bs, bc)) => {
                                if s > bs || (s == bs && c < bc) {
                                    best = Some((s, c))
                                }
                            }
                        }
                        cat_sum[c as usize] = 0;
                    }
                    cat_touched.clear();
                    row.top = best.map(|b| b.1);
                    rows.push(UserRow { ..*row });
                };
                let mut k = 0usize;
                while k < pe.len() {
                    let start = pe[k].ts;
                    let mut m = k + 1;
                    while m < pe.len() && pe[m].ts - pe[m - 1].ts <= GAP {
                        m += 1;
                    }
                    let s = &pe[k..m];
                    let day = start.div_euclid(DAY) as i32;
                    let di = (day as i64 - day0) as usize;
                    let mut rev = 0i64;
                    let mut purch = false;
                    for e in s {
                        if e.purchase {
                            purch = true;
                            rev += e.price as i64;
                            let c = e.cat as usize;
                            if cat_sum[c] == 0 {
                                cat_touched.push(e.cat);
                            }
                            cat_sum[c] += e.price as i64;
                        }
                    }
                    // credit
                    let ub = pc.partition_point(|c| c.ts <= start);
                    let mut camp = organic;
                    if ub > 0 && start - pc[ub - 1].ts <= WEEK {
                        let t = pc[ub - 1].ts;
                        let lb = pc[..ub].partition_point(|c| c.ts < t);
                        camp = pc[lb].camp;
                    }
                    let a = &mut camp_acc[camp as usize * ndays + di];
                    a[0] += 1;
                    a[1] += purch as i64;
                    a[2] += rev;
                    // assists: clicks with start - WEEK <= ts <= start
                    while hi < pc.len() && pc[hi].ts <= start {
                        let c = pc[hi].camp as usize;
                        if cnt[c] == 0 {
                            pos[c] = active.len() as u32;
                            active.push(c as u16);
                        }
                        cnt[c] += 1;
                        hi += 1;
                    }
                    while lo < hi && pc[lo].ts < start - WEEK {
                        let c = pc[lo].camp as usize;
                        cnt[c] -= 1;
                        if cnt[c] == 0 {
                            let pi = pos[c] as usize;
                            let last = *active.last().unwrap();
                            active[pi] = last;
                            pos[last as usize] = pi as u32;
                            active.pop();
                            pos[c] = u32::MAX;
                        }
                        lo += 1;
                    }
                    for &c in &active {
                        camp_acc[c as usize * ndays + di][3] += 1;
                    }
                    // user-day accumulation (cat sums already include this session; days only move forward)
                    if day != cur_day {
                        cur_day = day;
                        row = UserRow { person: p, day, sessions: 0, events: 0, revenue: 0, top: None };
                    }
                    row.sessions += 1;
                    row.events += s.len() as i64;
                    row.revenue += rev;
                    // the row ends when this person has no more sessions or the next one starts on a new day
                    if m >= pe.len() || next_session_day(pe, m) != day {
                        flush(&mut row, &mut cat_sum, &mut cat_touched, &mut rows);
                    }
                    k = m;
                }
                // reset assist window state
                for &c in &active {
                    cnt[c as usize] = 0;
                    pos[c as usize] = u32::MAX;
                }
                active.clear();
                i = j;
                ci = cj;
            }
            (rows, camp_acc)
        })
        .collect();
    log(&t0, "sessions + attribution");

    // ---- outputs
    let mut camp_tot: Vec<[i64; 4]> = vec![[0; 4]; ncamp * ndays];
    let mut user_parts: Vec<Vec<UserRow>> = vec![];
    for (rows, acc) in results {
        for (t, a) in camp_tot.iter_mut().zip(acc.iter()) {
            for q in 0..4 {
                t[q] += a[q];
            }
        }
        user_parts.push(rows);
    }
    let ud = out_dir.join("user_daily");
    let cd = out_dir.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();

    let nfiles = 8usize;
    let per = (user_parts.len() + nfiles - 1) / nfiles;
    let groups: Vec<Vec<Vec<UserRow>>> = {
        let mut g: Vec<Vec<Vec<UserRow>>> = (0..nfiles).map(|_| vec![]).collect();
        for (i, p) in user_parts.into_iter().enumerate() {
            g[i / per.max(1)].push(p);
        }
        g
    };
    let cat_names_ref = &cat_names;
    groups.into_par_iter().enumerate().for_each(|(gi, g)| {
        let rows: Vec<&UserRow> = g.iter().flatten().collect();
        let schema = Arc::new(
            parse_message_type(
                "message user_daily {
                    REQUIRED BYTE_ARRAY user_id (UTF8);
                    REQUIRED INT32 day (DATE);
                    REQUIRED INT64 sessions;
                    REQUIRED INT64 events;
                    REQUIRED INT64 revenue_cents;
                    OPTIONAL BYTE_ARRAY top_category (UTF8);
                }",
            )
            .unwrap(),
        );
        let props = Arc::new(WriterProperties::builder().set_dictionary_enabled(false).build());
        let f = File::create(ud.join(format!("part-{gi}.parquet"))).unwrap();
        let mut w = SerializedFileWriter::new(f, schema, props).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let mut col = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            match col {
                0 => {
                    let v: Vec<ByteArray> = rows.iter().map(|r| ByteArray::from(r.person.to_vec())).collect();
                    c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                    c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                2 | 3 | 4 => {
                    let v: Vec<i64> = rows
                        .iter()
                        .map(|r| match col {
                            2 => r.sessions,
                            3 => r.events,
                            _ => r.revenue,
                        })
                        .collect();
                    c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                _ => {
                    let defs: Vec<i16> = rows.iter().map(|r| r.top.is_some() as i16).collect();
                    let v: Vec<ByteArray> =
                        rows.iter().filter_map(|r| r.top.map(|t| ByteArray::from(cat_names_ref[t as usize].to_vec()))).collect();
                    c.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None).unwrap();
                }
            }
            c.close().unwrap();
            col += 1;
        }
        rg.close().unwrap();
        w.close().unwrap();
    });

    // campaign_daily
    {
        let mut cid: Vec<ByteArray> = vec![];
        let mut chan: Vec<ByteArray> = vec![];
        let mut day: Vec<i32> = vec![];
        let mut cols: [Vec<i64>; 4] = Default::default();
        for c in 0..ncamp {
            for d in 0..ndays {
                let a = camp_tot[c * ndays + d];
                if a[0] == 0 && a[3] == 0 {
                    continue;
                }
                if c == organic as usize {
                    cid.push(ByteArray::from("organic"));
                    chan.push(ByteArray::from("organic"));
                } else {
                    cid.push(ByteArray::from(camp_rows[c].0.to_vec()));
                    chan.push(ByteArray::from(camp_rows[c].1.to_vec()));
                }
                day.push((day0 + d as i64) as i32);
                for q in 0..4 {
                    cols[q].push(a[q]);
                }
            }
        }
        let schema = Arc::new(
            parse_message_type(
                "message campaign_daily {
                    REQUIRED BYTE_ARRAY campaign_id (UTF8);
                    REQUIRED BYTE_ARRAY channel (UTF8);
                    REQUIRED INT32 day (DATE);
                    REQUIRED INT64 sessions;
                    REQUIRED INT64 purchasing_sessions;
                    REQUIRED INT64 revenue_cents;
                    REQUIRED INT64 assisted_sessions;
                }",
            )
            .unwrap(),
        );
        let props = Arc::new(WriterProperties::builder().build());
        let f = File::create(cd.join("part-0.parquet")).unwrap();
        let mut w = SerializedFileWriter::new(f, schema, props).unwrap();
        let mut rg = w.next_row_group().unwrap();
        let mut col = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            match col {
                0 => {
                    c.typed::<ByteArrayType>().write_batch(&cid, None, None).unwrap();
                }
                1 => {
                    c.typed::<ByteArrayType>().write_batch(&chan, None, None).unwrap();
                }
                2 => {
                    c.typed::<Int32Type>().write_batch(&day, None, None).unwrap();
                }
                k => {
                    c.typed::<Int64Type>().write_batch(&cols[k - 3], None, None).unwrap();
                }
            }
            c.close().unwrap();
            col += 1;
        }
        rg.close().unwrap();
        w.close().unwrap();
    }
    log(&t0, "done");
    std::process::exit(0);
}

/// Day (UTC) of the session starting at index m.
#[inline]
fn next_session_day(pe: &[Ev], m: usize) -> i32 {
    pe[m].ts.div_euclid(DAY) as i32
}
