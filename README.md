# H1B Email Leadgen

Standalone automation to generate 30-40 recruiter/hiring contacts for non-FAANG H1B sponsor companies.

## Run

```bash
npm run leads:generate
```

Outputs:

- `output/h1b-recruiter-leads.json`
- `output/h1b-recruiter-leads.csv`

## Optional better emails

Add `.env` with:

```bash
HUNTER_API_KEY=your_key_here
LEADS_TARGET=40
```

Without `HUNTER_API_KEY`, the script still returns role-based aliases like `careers@company.com`, `recruiting@company.com`.
