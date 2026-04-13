import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const COMPANY_FILE = path.resolve("data/h1b-midmarket-companies.json");
const OUTPUT_DIR = path.resolve("output");
const FALLBACK_ALIASES = ["careers", "jobs", "recruiting", "talent", "hr"];
const ROLE_KEYWORDS = [
  "recruiter",
  "talent",
  "hiring manager",
  "engineering manager",
  "data science manager",
  "analytics manager"
];

function normalize(value) {
  return (value || "").toString().trim().toLowerCase();
}

async function loadEnv() {
  const envPath = path.resolve(".env");
  try {
    const content = await readFile(envPath, "utf8");
    for (const line of content.split("\n")) {
      const entry = line.trim();
      if (!entry || entry.startsWith("#")) continue;
      const index = entry.indexOf("=");
      if (index <= 0) continue;
      const key = entry.slice(0, index).trim();
      const value = entry.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
      if (!process.env[key]) process.env[key] = value;
    }
  } catch {
    // Optional .env
  }
}

function hasRelevantRole(position) {
  const title = normalize(position);
  return ROLE_KEYWORDS.some((term) => title.includes(term));
}

function signalScore(signal) {
  if (signal === "high") return 20;
  if (signal === "medium") return 14;
  return 8;
}

function roleScore(position) {
  const title = normalize(position);
  if (title.includes("recruiter")) return 28;
  if (title.includes("talent")) return 24;
  if (title.includes("hiring manager")) return 22;
  if (title.includes("manager")) return 18;
  return 10;
}

function verificationScore(status) {
  const value = normalize(status);
  if (value === "valid") return 10;
  if (value === "accept_all") return 7;
  if (value === "webmail") return 5;
  return 2;
}

function fitScore(company, person) {
  return Math.max(
    0,
    Math.min(100, signalScore(company.h1bSignal) + roleScore(person.position) + verificationScore(person.verification?.status))
  );
}

async function domainSearch(domain, apiKey) {
  const url = new URL("https://api.hunter.io/v2/domain-search");
  url.searchParams.set("domain", domain);
  url.searchParams.set("limit", "100");
  url.searchParams.set("api_key", apiKey);

  const response = await fetch(url);
  if (!response.ok) throw new Error(`Hunter ${response.status}`);
  const body = await response.json();
  return body?.data?.emails ?? [];
}

function toCsv(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const esc = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;
  const data = rows.map((row) => headers.map((h) => esc(row[h])).join(","));
  return [headers.join(","), ...data].join("\n");
}

async function main() {
  await loadEnv();
  const apiKey = process.env.HUNTER_API_KEY;
  const leadTarget = Number.parseInt(process.env.LEADS_TARGET || "40", 10);
  const companies = JSON.parse(await readFile(COMPANY_FILE, "utf8"));
  const allLeads = [];

  for (const company of companies) {
    if (apiKey) {
      try {
        const contacts = await domainSearch(company.domain, apiKey);
        for (const contact of contacts) {
          if (!hasRelevantRole(contact.position)) continue;
          allLeads.push({
            company: company.company,
            domain: company.domain,
            h1bSignal: company.h1bSignal,
            fullName: `${contact.first_name || ""} ${contact.last_name || ""}`.trim(),
            title: contact.position || "",
            email: contact.value || "",
            verificationStatus: contact.verification?.status || "unknown",
            source: "hunter-domain-search",
            score: fitScore(company, contact)
          });
        }
      } catch (error) {
        console.warn(`[warn] ${company.company}: ${error.message}`);
      }
    } else {
      for (const alias of FALLBACK_ALIASES) {
        allLeads.push({
          company: company.company,
          domain: company.domain,
          h1bSignal: company.h1bSignal,
          fullName: "",
          title: "Talent / Recruiting Alias",
          email: `${alias}@${company.domain}`,
          verificationStatus: "unverified",
          source: "generated-alias",
          score: signalScore(company.h1bSignal) + 4
        });
      }
    }
  }

  const deduped = Array.from(new Map(allLeads.map((lead) => [lead.email.toLowerCase(), lead])).values());
  const ranked = deduped.sort((a, b) => b.score - a.score).slice(0, leadTarget);

  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeFile(path.join(OUTPUT_DIR, "h1b-recruiter-leads.json"), JSON.stringify(ranked, null, 2), "utf8");
  await writeFile(path.join(OUTPUT_DIR, "h1b-recruiter-leads.csv"), toCsv(ranked), "utf8");

  if (!apiKey) {
    console.log("No HUNTER_API_KEY found. Generated alias emails.");
  }
  console.log(`Generated ${ranked.length} leads.`);
}

main().catch((error) => {
  console.error(`[error] ${error.message}`);
  process.exit(1);
});
