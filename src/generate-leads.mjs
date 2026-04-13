import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const OUTPUT_DIR = path.resolve("output");
const APOLLO_BASE = "https://api.apollo.io/api/v1";
const FALLBACK_ALIASES = ["careers", "jobs", "recruiting", "talent", "hr"];
const ROLE_KEYWORDS = [
  "recruiter",
  "talent",
  "hiring manager",
  "people partner",
  "people ops",
  "engineering manager",
  "data science manager",
  "analytics manager",
  "head of talent",
  "technical recruiter",
  "sourcer"
];
const ALT_ROLE_KEYWORDS = [
  "director",
  "manager",
  "head",
  "vp",
  "engineering",
  "data",
  "analytics",
  "machine learning"
];

/** Titles used in Apollo People API Search (technical recruiting focus). */
const APOLLO_RECRUITER_TITLES = [
  "technical recruiter",
  "engineering recruiter",
  "software recruiter",
  "technical sourcer",
  "technical talent acquisition",
  "talent acquisition engineer",
  "engineering talent acquisition",
  "technical recruiting",
  "recruiter",
  "talent acquisition"
];

function normalize(value) {
  return (value || "").toString().trim().toLowerCase();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function hasAlternativeRole(position) {
  const title = normalize(position);
  return ALT_ROLE_KEYWORDS.some((term) => title.includes(term));
}

function isTechnicalRecruitingTitle(title) {
  const t = normalize(title);
  if (!t.includes("recruit") && !t.includes("talent") && !t.includes("sourcer") && !t.includes("people")) return false;
  if (t.includes("technical") || t.includes("engineering") || t.includes("software") || t.includes("tech "))
    return true;
  if (t.includes("talent acquisition")) return true;
  return t.includes("recruiter") || t.includes("sourcer");
}

function signalScore(signal) {
  if (signal === "high") return 20;
  if (signal === "medium") return 14;
  return 8;
}

function roleScore(position) {
  const title = normalize(position);
  if (title.includes("technical recruiter") || title.includes("engineering recruiter")) return 32;
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

function emailDomainFromAddress(email) {
  const e = (email || "").trim().toLowerCase();
  const i = e.indexOf("@");
  if (i < 0) return "";
  return e.slice(i + 1).replace(/^www\./, "");
}

async function loadRecruitingFirmRules() {
  const filePath = path.resolve(process.env.RECRUITING_BLOCKLIST_FILE || "data/recruiting-firm-blocklist.json");
  try {
    const raw = await readFile(filePath, "utf8");
    const data = JSON.parse(raw);
    const domains = new Set((data.domains || []).map((d) => normalize(d).replace(/^www\./, "")));
    const organizationNameContains = (data.organizationNameContains || []).map((s) => normalize(s));
    return { domains, organizationNameContains };
  } catch {
    return { domains: new Set(), organizationNameContains: [] };
  }
}

/**
 * Classify whether a row is tied to a staffing / RPO / recruiting marketplace (see blocklist JSON).
 * Does not drop rows — use splitOutputs() to route to separate files.
 */
function classifyRecruitingAgencyLead(lead, rules) {
  const reasons = [];
  const targetDomain = normalize(lead.domain || "").replace(/^www\./, "");
  if (targetDomain && rules.domains.has(targetDomain)) reasons.push("target_company_domain");

  const companyName = normalize(lead.company || "");
  for (const frag of rules.organizationNameContains) {
    if (frag && companyName.includes(frag)) {
      reasons.push("target_company_name");
      break;
    }
  }

  const mailDomain = emailDomainFromAddress(lead.email);
  if (mailDomain && rules.domains.has(mailDomain)) reasons.push("contact_email_domain");

  const orgNm = normalize(lead.organizationName || "");
  for (const frag of rules.organizationNameContains) {
    if (frag && orgNm.includes(frag)) {
      reasons.push("employer_org_name");
      break;
    }
  }

  const unique = [...new Set(reasons)];
  return { isAgency: unique.length > 0, reasons: unique };
}

async function writeLeadOutputs(employerLeads, agencyRows, outputBasename, recruitingBasename) {
  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeFile(path.join(OUTPUT_DIR, `${outputBasename}.json`), JSON.stringify(employerLeads, null, 2), "utf8");
  await writeFile(path.join(OUTPUT_DIR, `${outputBasename}.csv`), toCsv(employerLeads), "utf8");

  await writeFile(path.join(OUTPUT_DIR, `${recruitingBasename}.json`), JSON.stringify(agencyRows, null, 2), "utf8");
  await writeFile(
    path.join(OUTPUT_DIR, `${recruitingBasename}.csv`),
    agencyRows.length ? toCsv(agencyRows) : "",
    "utf8"
  );
}

function parseProviders(hunterKey, apolloKey) {
  const raw = (process.env.PROVIDERS || "").toLowerCase().trim();
  if (raw) {
    const set = new Set(
      raw
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    );
    return set;
  }
  const auto = new Set();
  if (hunterKey) auto.add("hunter");
  if (apolloKey) auto.add("apollo");
  return auto;
}

async function domainSearch(domain, apiKey, limit = 10, offset = 0) {
  const url = new URL("https://api.hunter.io/v2/domain-search");
  url.searchParams.set("domain", domain);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("api_key", apiKey);

  const response = await fetch(url);
  if (!response.ok) throw new Error(`Hunter ${response.status}`);
  const body = await response.json();
  return body?.data?.emails ?? [];
}

/**
 * Apollo People API Search — does not return emails; use people/match after.
 * @see https://docs.apollo.io/reference/people-api-search
 */
async function apolloPeopleSearch(apolloKey, domain, page, perPage) {
  const body = {
    q_organization_domains_list: [domain],
    person_titles: APOLLO_RECRUITER_TITLES,
    per_page: perPage,
    page
  };
  const response = await fetch(`${APOLLO_BASE}/mixed_people/api_search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      "Cache-Control": "no-cache",
      "X-Api-Key": apolloKey
    },
    body: JSON.stringify(body)
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Apollo search ${response.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

/**
 * Apollo People Enrichment — returns full person including work email when available.
 * @see https://docs.apollo.io/reference/people-enrichment
 */
async function apolloPeopleMatch(apolloKey, personId) {
  const revealPersonal = process.env.APOLLO_REVEAL_PERSONAL_EMAILS === "true";
  const params = new URLSearchParams({
    id: personId,
    reveal_personal_emails: String(revealPersonal)
  });
  const response = await fetch(`${APOLLO_BASE}/people/match?${params.toString()}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      "Cache-Control": "no-cache",
      "X-Api-Key": apolloKey
    }
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Apollo match ${response.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

function pickEmailFromApolloPerson(person) {
  if (!person || typeof person !== "object") return "";
  const direct =
    person.email ||
    person.corporate_email ||
    person.sanitized_email ||
    person.professional_email ||
    "";
  if (direct) return String(direct).trim();
  const emails = person.emails;
  if (Array.isArray(emails) && emails[0]?.email) return String(emails[0].email).trim();
  return "";
}

function apolloOrgDomainMatchesCompany(org, companyDomain) {
  const raw =
    org.primary_domain ||
    org.domain ||
    (typeof org.website_url === "string" ? org.website_url.replace(/^https?:\/\//i, "").split("/")[0] : "") ||
    "";
  const d = normalize(raw).replace(/^www\./, "");
  const want = normalize(companyDomain).replace(/^www\./, "");
  if (!d || !want) return true;
  return d === want || d.endsWith(`.${want}`) || want.endsWith(`.${d}`);
}

function apolloPersonToLead(company, person, emailSourceDetail) {
  const org = person.organization || {};
  if (!apolloOrgDomainMatchesCompany(org, company.domain)) {
    return null;
  }
  const title = person.title || "";
  const email = pickEmailFromApolloPerson(person);
  const fakeVerification = email ? "apollo_matched" : "no_email_in_response";

  return {
    company: company.company,
    domain: company.domain,
    h1bSignal: company.h1bSignal,
    fullName: [person.first_name, person.last_name].filter(Boolean).join(" ").trim(),
    title,
    email,
    emailSource: "Apollo",
    source: emailSourceDetail,
    verificationStatus: fakeVerification,
    apolloPersonId: person.id || "",
    linkedinUrl: person.linkedin_url || "",
    city: person.city || "",
    state: person.state || "",
    country: person.country || "",
    phone: person.sanitized_phone || person.phone_numbers?.[0]?.raw_number || person.phone_numbers?.[0]?.sanitized_number || "",
    seniority: person.seniority || "",
    departments: Array.isArray(person.departments) ? person.departments.join("; ") : String(person.departments || ""),
    organizationName: org.name || "",
    organizationApolloId: org.id || "",
    employeeCount: org.estimated_num_employees != null ? String(org.estimated_num_employees) : "",
    score: Math.min(
      100,
      signalScore(company.h1bSignal) +
        roleScore(title) +
        (isTechnicalRecruitingTitle(title) ? 8 : 0) +
        (email ? 6 : 0)
    )
  };
}

function hunterContactToLead(company, contact, sourceDetail) {
  return {
    company: company.company,
    domain: company.domain,
    h1bSignal: company.h1bSignal,
    fullName: `${contact.first_name || ""} ${contact.last_name || ""}`.trim(),
    title: contact.position || "",
    email: contact.value || "",
    emailSource: "Hunter",
    source: sourceDetail,
    verificationStatus: contact.verification?.status || "unknown",
    apolloPersonId: "",
    linkedinUrl: "",
    city: "",
    state: "",
    country: "",
    phone: "",
    seniority: "",
    departments: "",
    organizationName: "",
    organizationApolloId: "",
    employeeCount: "",
    score: fitScore(company, contact)
  };
}

function toCsv(rows) {
  if (!rows.length) return "";
  const headers = Object.keys(rows[0]);
  const esc = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;
  const data = rows.map((row) => headers.map((h) => esc(row[h])).join(","));
  return [headers.join(","), ...data].join("\n");
}

async function collectHunterForCompany(company, hunterApiKey, maxPerCompany, hunterLimit, hunterPages) {
  const out = [];
  try {
    const contacts = [];
    for (let page = 0; page < hunterPages; page += 1) {
      const offset = page * hunterLimit;
      try {
        const batch = await domainSearch(company.domain, hunterApiKey, hunterLimit, offset);
        if (!batch.length) break;
        contacts.push(...batch);
        if (batch.length < hunterLimit) break;
      } catch (pageError) {
        if (page > 0) break;
        throw pageError;
      }
    }
    const strictMatches = [];
    const altMatches = [];
    for (const contact of contacts) {
      const candidate = hunterContactToLead(company, contact, "hunter-domain-search");
      if (hasRelevantRole(contact.position)) {
        strictMatches.push(candidate);
        continue;
      }
      if (hasAlternativeRole(contact.position)) {
        altMatches.push(candidate);
      }
    }

    const selected = strictMatches.length > 0 ? strictMatches : altMatches;
    selected
      .sort((a, b) => b.score - a.score)
      .slice(0, maxPerCompany)
      .forEach((lead) => out.push(lead));

    if (selected.length === 0 && contacts.length > 0) {
      const fallback = contacts
        .map((c) => hunterContactToLead(company, c, "hunter-domain-search-fallback"))
        .sort((a, b) => b.score - a.score)[0];
      if (fallback) out.push(fallback);
    }
  } catch (error) {
    console.warn(`[warn] Hunter ${company.company}: ${error.message}`);
  }
  return out;
}

async function collectApolloForCompany(company, apolloKey, maxPerCompany, delayMs, globalMatchBudget) {
  const out = [];
  if (globalMatchBudget.value <= 0) return out;

  await sleep(delayMs);
  const perPage = Math.min(25, Math.max(maxPerCompany * 4, 10));
  let searchJson;
  try {
    searchJson = await apolloPeopleSearch(apolloKey, company.domain, 1, perPage);
  } catch (error) {
    console.warn(`[warn] Apollo search ${company.company}: ${error.message}`);
    return out;
  }

  const people = searchJson?.people || [];
  const ranked = [...people].sort((a, b) => {
    const ta = isTechnicalRecruitingTitle(a.title) ? 1 : 0;
    const tb = isTechnicalRecruitingTitle(b.title) ? 1 : 0;
    if (tb !== ta) return tb - ta;
    return (b.has_email ? 1 : 0) - (a.has_email ? 1 : 0);
  });

  let matched = 0;
  for (const p of ranked) {
    if (matched >= maxPerCompany || globalMatchBudget.value <= 0) break;
    const pid = p.id || p.person_id;
    if (!pid) continue;
    await sleep(delayMs);
    try {
      const enriched = await apolloPeopleMatch(apolloKey, pid);
      globalMatchBudget.value -= 1;
      const person = enriched?.person;
      if (!person) continue;
      const lead = apolloPersonToLead(company, person, "apollo-people-match");
      if (!lead) continue;
      if (!lead.email && process.env.APOLLO_SKIP_NO_EMAIL === "true") continue;
      out.push(lead);
      matched += 1;
    } catch (error) {
      console.warn(`[warn] Apollo match ${company.company} (${pid}): ${error.message}`);
    }
  }
  return out;
}

async function main() {
  await loadEnv();
  const hunterApiKey = process.env.HUNTER_API_KEY;
  const apolloApiKey = process.env.APOLLO_API_KEY;

  const companyFile = path.resolve(process.env.COMPANY_FILE || "data/h1b-midmarket-companies.json");
  const outputBasename =
    process.env.OUTPUT_BASENAME ||
    (apolloApiKey ? "technical-recruiter-leads" : "h1b-recruiter-leads");
  const recruitingOutputBasename = process.env.RECRUITING_OUTPUT_BASENAME || "recruiting-firm-leads";

  const requirePersonEmails = (process.env.REQUIRE_PERSON_EMAILS || "true").toLowerCase() !== "false";
  const leadTarget = Number.parseInt(process.env.LEADS_TARGET || "40", 10);
  const maxPerCompany = Number.parseInt(process.env.MAX_PER_COMPANY || "1", 10);
  const hunterLimit = Number.parseInt(process.env.HUNTER_LIMIT || "10", 10);
  const hunterPages = Number.parseInt(process.env.HUNTER_PAGES || "3", 10);
  const apolloDelayMs = Number.parseInt(process.env.APOLLO_DELAY_MS || "2500", 10);
  const apolloMatchBudget = { value: Number.parseInt(process.env.APOLLO_MAX_MATCHES_GLOBAL || "80", 10) };

  const companies = JSON.parse(await readFile(companyFile, "utf8"));
  const allLeads = [];

  if (!hunterApiKey && !apolloApiKey) {
    for (const company of companies) {
      for (const alias of FALLBACK_ALIASES) {
        allLeads.push({
          company: company.company,
          domain: company.domain,
          h1bSignal: company.h1bSignal,
          fullName: "",
          title: "Talent / Recruiting Alias",
          email: `${alias}@${company.domain}`,
          emailSource: "Generated",
          source: "generated-alias",
          verificationStatus: "unverified",
          apolloPersonId: "",
          linkedinUrl: "",
          city: "",
          state: "",
          country: "",
          phone: "",
          seniority: "",
          departments: "",
          organizationName: "",
          organizationApolloId: "",
          employeeCount: "",
          score: signalScore(company.h1bSignal) + 4
        });
      }
    }
    const dedupedAlias = Array.from(new Map(allLeads.map((lead) => [lead.email.toLowerCase(), lead])).values());
    const withEmailAlias = dedupedAlias.filter((lead) => (lead.email || "").trim().length > 0);
    const rulesAlias = await loadRecruitingFirmRules();
    const classifiedAlias = withEmailAlias.map((lead) => {
      const { isAgency, reasons } = classifyRecruitingAgencyLead(lead, rulesAlias);
      return { lead, isAgency, reasonStr: reasons.join("; ") };
    });
    const employerAlias = classifiedAlias.filter((x) => !x.isAgency).map((x) => x.lead);
    const agencyAlias = classifiedAlias
      .filter((x) => x.isAgency)
      .map((x) => ({
        ...x.lead,
        recruitingAgencyMatch: "yes",
        recruitingAgencyReason: x.reasonStr
      }));
    const rankedAlias = employerAlias.sort((a, b) => b.score - a.score).slice(0, leadTarget);
    await writeLeadOutputs(rankedAlias, agencyAlias, outputBasename, recruitingOutputBasename);
    console.log(`No API keys — ${rankedAlias.length} employer alias leads → output/${outputBasename}.csv`);
    console.log(`Split ${agencyAlias.length} staffing/RPO alias leads → output/${recruitingOutputBasename}.csv`);
    return;
  }

  const providers = parseProviders(hunterApiKey, apolloApiKey);

  for (const company of companies) {
    if (providers.has("hunter") && hunterApiKey) {
      const hunterLeads = await collectHunterForCompany(company, hunterApiKey, maxPerCompany, hunterLimit, hunterPages);
      allLeads.push(...hunterLeads);
    }

    if (providers.has("apollo") && apolloApiKey) {
      const apolloLeads = await collectApolloForCompany(company, apolloApiKey, maxPerCompany, apolloDelayMs, apolloMatchBudget);
      allLeads.push(...apolloLeads);
    }
  }

  function dedupeKey(lead) {
    const e = (lead.email || "").trim().toLowerCase();
    if (e) return `email:${e}`;
    if (lead.apolloPersonId) return `apollo:${lead.apolloPersonId}`;
    return `row:${lead.company}:${lead.fullName}:${lead.title}`;
  }

  const deduped = Array.from(new Map(allLeads.map((lead) => [dedupeKey(lead), lead])).values());
  const withEmail = deduped.filter((lead) => (lead.email || "").trim().length > 0);
  const rules = await loadRecruitingFirmRules();
  const classified = withEmail.map((lead) => {
    const { isAgency, reasons } = classifyRecruitingAgencyLead(lead, rules);
    return { lead, isAgency, reasonStr: reasons.join("; ") };
  });
  const employerRaw = classified.filter((x) => !x.isAgency).map((x) => x.lead);
  const agencyRows = classified
    .filter((x) => x.isAgency)
    .map((x) => ({
      ...x.lead,
      recruitingAgencyMatch: "yes",
      recruitingAgencyReason: x.reasonStr
    }));

  const ranked = employerRaw.sort((a, b) => b.score - a.score).slice(0, leadTarget);
  const rankedAgency = agencyRows.sort((a, b) => b.score - a.score);

  await writeLeadOutputs(ranked, rankedAgency, outputBasename, recruitingOutputBasename);

  if (!hunterApiKey && !apolloApiKey && requirePersonEmails) {
    console.log("No API keys found. Generated alias emails only.");
  }
  console.log(`Generated ${ranked.length} employer leads → output/${outputBasename}.csv`);
  console.log(`Split ${rankedAgency.length} staffing/RPO leads → output/${recruitingOutputBasename}.csv`);
}

main().catch((error) => {
  console.error(`[error] ${error.message}`);
  process.exit(1);
});
