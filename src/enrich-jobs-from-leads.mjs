/**
 * Read a leads CSV/XLSX (from generate-leads output), fetch open roles per company domain,
 * and write a new file with all original columns plus up to 3 roles + links (or N/A).
 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import * as XLSX from "xlsx";
import * as cheerio from "cheerio";

const OUTPUT_DIR = path.resolve("output");

async function loadEnv() {
  const envPath = path.resolve(".env");
  try {
    const content = await readFile(envPath, "utf8");
    for (const line of content.split("\n")) {
      const entry = line.trim();
      if (!entry || entry.startsWith("#")) continue;
      const i = entry.indexOf("=");
      if (i <= 0) continue;
      const key = entry.slice(0, i).trim();
      const value = entry.slice(i + 1).trim().replace(/^['"]|['"]$/g, "");
      if (!process.env[key]) process.env[key] = value;
    }
  } catch {
    // optional
  }
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

async function fetchText(url, timeoutMs = 15000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": UA, Accept: "text/html,application/json,*/*" }
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

async function fetchJson(url, timeoutMs = 15000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": UA, Accept: "application/json" }
    });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(t);
  }
}

/** @returns {{ title: string, url: string }[]} max 3 */
async function jobsFromGreenhouseBoard(boardToken) {
  const url = `https://boards-api.greenhouse.io/v1/boards/${encodeURIComponent(boardToken)}/jobs?content=true`;
  const data = await fetchJson(url);
  if (!data?.jobs?.length) return [];
  return data.jobs.slice(0, 10).map((j) => ({
    title: String(j.title || "").trim(),
    url: String(j.absolute_url || j.url || "").trim()
  }));
}

/** @returns {{ title: string, url: string }[]} */
async function jobsFromLeverCompany(slug) {
  const url = `https://api.lever.co/v0/postings/${encodeURIComponent(slug)}?mode=json`;
  const data = await fetchJson(url);
  if (!data) return [];
  const list = Array.isArray(data) ? data : data.data || [];
  return list.slice(0, 10).map((p) => ({
    title: String(p.text || p.title || "").trim(),
    url: String(p.hostedUrl || p.applyUrl || "").trim()
  }));
}

function extractGreenhouseToken(html) {
  if (!html) return null;
  const patterns = [
    /boards\.greenhouse\.io\/([^/"'\s?]+)/i,
    /greenhouse\.io\/embed\/job_board\/js\?for=([^&"'\s]+)/i,
    /"board_token"\s*:\s*"([^"]+)"/i,
    /board_token\s*=\s*['"]([^'"]+)['"]/i
  ];
  for (const re of patterns) {
    const m = html.match(re);
    if (m?.[1] && m[1].length < 80) return m[1];
  }
  return null;
}

function extractLeverSlug(html) {
  if (!html) return null;
  const m = html.match(/jobs\.lever\.co\/([^/"'\s?]+)/i);
  return m?.[1] || null;
}

function extractAshbyOrg(html) {
  const m = html?.match(/jobs\.ashbyhq\.com\/([^/"'\s?]+)/i);
  return m?.[1] || null;
}

async function jobsFromAshbyPage(orgSlug) {
  const base = `https://jobs.ashbyhq.com/${encodeURIComponent(orgSlug)}`;
  const html = await fetchText(base);
  if (!html) return [];
  return jobsFromHtmlLinks(html, base);
}

const JOBISH = /(engineer|developer|analyst|scientist|data|software|machine|ml|backend|frontend|fullstack|product|design|recruit|talent|security|cloud|devops|sre|qa|ios|android)/i;

function pickBestJobs(candidates, limit = 3) {
  const scored = candidates
    .filter((j) => j.title && j.url)
    .map((j) => ({
      ...j,
      _s: JOBISH.test(j.title) ? 1 : 0
    }))
    .sort((a, b) => b._s - a._s);
  const seen = new Set();
  const out = [];
  for (const j of scored) {
    const key = j.url.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ title: j.title, url: j.url });
    if (out.length >= limit) break;
  }
  return out.slice(0, limit);
}

function looksLikeJobPostingUrl(absUrl) {
  let u;
  try {
    u = new URL(absUrl);
  } catch {
    return false;
  }
  const path = u.pathname.toLowerCase();
  const h = absUrl.toLowerCase();
  if (path.includes("state-of") || path.includes("/explore")) return false;
  if (
    /\/(blog|news|press|product|capability|pricing|customers|resources|solutions|webinar|events)(\/|$)/.test(path)
  )
    return false;
  if (
    /(greenhouse\.io\/|boards\.greenhouse|lever\.co\/|ashbyhq\.com\/|myworkdayjobs\.com|smartrecruiters\.com\/|icims\.com\/|rippling\.com\/careers)/.test(
      h
    )
  )
    return true;
  if (/\/(careers|job-openings|openings|positions)(\/|$)/.test(path)) return true;
  if (/\/jobs(\/|$)/.test(path) && !/\/jobs\/(state|blog|news)/.test(path)) return true;
  if (/\/job[s/-][^/]+/i.test(path) && !/product/.test(path)) return true;
  return false;
}

function jobsFromHtmlLinks(html, baseUrl) {
  const $ = cheerio.load(html);
  const candidates = [];
  $("a[href]").each((_, el) => {
    const href = $(el).attr("href") || "";
    const text = $(el).text().replace(/\s+/g, " ").trim();
    if (!text || text.length < 6 || text.length > 200) return;
    try {
      const abs = new URL(href, baseUrl).href;
      if (!looksLikeJobPostingUrl(abs)) return;
      if (!JOBISH.test(text) && !/recruit|talent|engineer|analyst|scientist|developer|manager|director/i.test(text))
        return;
      candidates.push({ title: text, url: abs });
    } catch {
      /* ignore */
    }
  });
  return pickBestJobs(candidates, 3);
}

async function discoverJobsForDomain(domain) {
  const clean = domain.replace(/^www\./i, "").trim();
  if (!clean) return [];

  const candidates = [];
  const tryUrls = [
    `https://${clean}/careers`,
    `https://www.${clean}/careers`,
    `https://${clean}/jobs`,
    `https://www.${clean}/jobs`,
    `https://jobs.${clean}/`,
    `https://apply.${clean}/`,
    `https://careers.${clean}/`,
    `https://www.${clean}/about/careers`
  ];

  let combinedHtml = "";

  for (const url of tryUrls) {
    const html = await fetchText(url);
    if (!html || html.length < 200) continue;
    combinedHtml += html;

    const gh = extractGreenhouseToken(html);
    if (gh) {
      const j = await jobsFromGreenhouseBoard(gh);
      candidates.push(...j.map((x) => ({ title: x.title, url: x.url })));
      if (candidates.length >= 3) return pickBestJobs(candidates, 3);
    }

    const lev = extractLeverSlug(html);
    if (lev) {
      const j = await jobsFromLeverCompany(lev);
      candidates.push(...j.map((x) => ({ title: x.title, url: x.url })));
      if (candidates.length >= 3) return pickBestJobs(candidates, 3);
    }

    const ash = extractAshbyOrg(html);
    if (ash) {
      const j = await jobsFromAshbyPage(ash);
      candidates.push(...j);
      if (candidates.length >= 3) return pickBestJobs(candidates, 3);
    }

    const generic = jobsFromHtmlLinks(html, url);
    candidates.push(...generic);
  }

  if (candidates.length) return pickBestJobs(candidates, 3);

  if (combinedHtml.length > 500) {
    const firstUrl = tryUrls[0];
    return jobsFromHtmlLinks(combinedHtml.slice(0, 500000), `https://${clean}/`);
  }

  return [];
}

function padJobs(jobs) {
  const slots = [
    { role: "N/A", url: "N/A" },
    { role: "N/A", url: "N/A" },
    { role: "N/A", url: "N/A" }
  ];
  for (let i = 0; i < Math.min(3, jobs.length); i++) {
    slots[i] = { role: jobs[i].title || "N/A", url: jobs[i].url || "N/A" };
  }
  return slots;
}

function summaryLine(slots) {
  const parts = slots
    .filter((s) => s.role !== "N/A" && s.url !== "N/A")
    .map((s) => `${s.role} — ${s.url}`);
  return parts.length ? parts.join(" | ") : "N/A";
}

async function readSheetRows(filePath) {
  const buf = await readFile(filePath);
  const ext = path.extname(filePath).toLowerCase();
  let wb;
  if (ext === ".csv") {
    const text = buf.toString("utf8");
    wb = XLSX.read(text, { type: "string" });
  } else {
    wb = XLSX.read(buf, { type: "buffer" });
  }
  const sheetName = wb.SheetNames[0];
  const sheet = wb.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(sheet, { defval: "" });
}

async function main() {
  await loadEnv();
  const inputFile = path.resolve(process.env.JOBS_INPUT_FILE || "output/technical-recruiter-leads.csv");
  const outputBase = path.resolve(process.env.JOBS_OUTPUT_BASENAME || path.join(OUTPUT_DIR, "leads-with-open-roles"));
  const delayMs = Number.parseInt(process.env.JOBS_FETCH_DELAY_MS || "2000", 10);

  let rows = await readSheetRows(inputFile);
  if (!rows.length) {
    console.error("No rows in input file.");
    process.exit(1);
  }
  const maxRows = process.env.JOBS_MAX_ROWS ? Number.parseInt(process.env.JOBS_MAX_ROWS, 10) : rows.length;
  if (Number.isFinite(maxRows) && maxRows > 0 && maxRows < rows.length) {
    rows = rows.slice(0, maxRows);
    console.log(`Using first ${maxRows} rows (JOBS_MAX_ROWS).`);
  }

  const domainCache = new Map();

  const enriched = [];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const domain = String(row.domain || row.Domain || "").trim();
    if (!domain) {
      const slots = padJobs([]);
      enriched.push({
        ...row,
        open_role_1: slots[0].role,
        open_role_1_url: slots[0].url,
        open_role_2: slots[1].role,
        open_role_2_url: slots[1].url,
        open_role_3: slots[2].role,
        open_role_3_url: slots[2].url,
        open_roles_summary: "N/A"
      });
      continue;
    }

    if (!domainCache.has(domain)) {
      if (domainCache.size > 0) await sleep(delayMs);
      const jobs = await discoverJobsForDomain(domain);
      domainCache.set(domain, jobs);
      console.log(`[${i + 1}/${rows.length}] ${domain} → ${jobs.length || 0} role(s) found`);
    }

    const jobs = domainCache.get(domain) || [];
    const slots = padJobs(jobs);
    enriched.push({
      ...row,
      open_role_1: slots[0].role,
      open_role_1_url: slots[0].url,
      open_role_2: slots[1].role,
      open_role_2_url: slots[1].url,
      open_role_3: slots[2].role,
      open_role_3_url: slots[2].url,
      open_roles_summary: summaryLine(slots)
    });
  }

  await mkdir(OUTPUT_DIR, { recursive: true });
  const ws = XLSX.utils.json_to_sheet(enriched);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Leads");
  const xlsxPath = `${outputBase}.xlsx`;
  XLSX.writeFile(wb, xlsxPath);

  const csv = XLSX.utils.sheet_to_csv(ws);
  const csvPath = `${outputBase}.csv`;
  await writeFile(csvPath, csv, "utf8");

  console.log(`Wrote ${xlsxPath}`);
  console.log(`Wrote ${csvPath}`);
}

main().catch((e) => {
  console.error(e?.message || e);
  process.exit(1);
});
