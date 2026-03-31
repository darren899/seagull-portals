# Seagull Maritime — Portals

Self-hosted compliance portals for Seagull Maritime Ltd.

## Structure

```
seagull-portals/
├── index.html                     ← Landing page (links to both portals)
├── .nojekyll                      ← Tells GitHub Pages to serve as-is
├── dd/                            ← Due Diligence Document Portal
│   ├── index.html                 ← Portal with access gate + document viewer
│   └── docs/                      ← PDF documents organised by section
│       ├── s1/                    ← Company Registration & Information
│       │   ├── profile/
│       │   ├── registration/
│       │   └── financial/
│       ├── s2/                    ← Compliance & Accreditation
│       │   ├── accreditations/
│       │   ├── flags/
│       │   ├── liberia/
│       │   ├── panama/
│       │   └── palau/
│       ├── s3/                    ← Standard Operating Procedures
│       ├── s4/                    ← Rules on Use of Force
│       ├── s5/                    ← Management Procedures
│       ├── s6/                    ← Personnel Management
│       │   ├── example/
│       │   └── mso-docs/
│       ├── s7/                    ← Weapons Management
│       │   ├── gbsit163/ - gbsit039/
│       │   └── eu1350/ - eu1352/
│       ├── s8/                    ← Company Policies & Procedures
│       │   └── policies/
│       ├── s9/                    ← Testimonials & References
│       │   └── ptr/
│       └── s10/                   ← Insurance
└── campaign/                      ← "Your Watch. Our Standard." Campaign
    ├── index.html                 ← Campaign portal (internal)
    ├── assets/                    ← Brand assets (logos, SVGs)
    ├── bulletins/                 ← Awareness bulletins
    │   └── bulletin-001-vessel-transfers.html
    └── flash-cards/               ← Printable toolbox talk flash cards
        └── flash-cards-vessel-transfers.html
```

## Setup — GitHub Pages

### 1. Create the repository

```bash
cd seagull-portals
git init
git add .
git commit -m "Initial portal deployment"
git remote add origin https://github.com/darren899/seagull-portals.git
git branch -M main
git push -u origin main
```

### 2. Enable GitHub Pages

1. Go to https://github.com/darren899/seagull-portals/settings/pages
2. Under **Source**, select **Deploy from a branch**
3. Select **main** branch, **/ (root)** folder
4. Click **Save**

The site will be live at: `https://darren899.github.io/seagull-portals/`

### 3. Add DD documents

Drop the actual PDF files into the matching `dd/docs/` folders. The folder structure matches the sections in the portal:

| Portal Section | Folder | What goes here |
|---|---|---|
| 1. Company Registration | `s1/profile/`, `s1/registration/`, `s1/financial/` | Company profile, certificates, financials |
| 2. Compliance & Accreditation | `s2/accreditations/`, `s2/flags/`, etc. | ISO certs, flag state auths |
| 3. SOPs | `s3/` | Embarked teams manual, SOPs |
| 4. Use of Force | `s4/` | RUF documents |
| 5. Management Procedures | `s5/` | Doc control, risk assessment, crisis procedures |
| 6. Personnel Management | `s6/`, `s6/mso-docs/` | Training, recruitment, sample files |
| 7. Weapons Management | `s7/gbsit*/`, `s7/eu*/` | Export licences, EUCs, inventory |
| 8. Policies | `s8/policies/` | All company policies |
| 9. Testimonials | `s9/`, `s9/ptr/` | Client letters, PTRs |
| 10. Insurance | `s10/` | Insurance certificates |

### 4. Custom domain (later)

When IT sorts DNS access, add a CNAME file:

```bash
echo "portals.seagullmaritimeltd.com" > CNAME
git add CNAME && git commit -m "Add custom domain" && git push
```

Then configure DNS: CNAME record pointing `portals` to `darren899.github.io`

Subdomains could be:
- `portals.seagullmaritimeltd.com` — landing page
- Or use path-based: `portals.seagullmaritimeltd.com/dd/` and `portals.seagullmaritimeltd.com/campaign/`

## Access Codes (DD Portal)

The DD portal uses auto-rotating weekly access codes:

- Format: `SG` + Monday date of current week (`DDMMYY`)
- Example: Week of 31 March 2026 = `SG310326`
- Internal staff can calculate from the date
- Clients request current code from Commercial team
- Manual override code for demos: `DEMO2026` (expires end of 2026)

## Notes

- The `.nojekyll` file tells GitHub Pages to serve files as-is (no Jekyll processing)
- All portals are self-contained HTML — no build step required
- DD portal has anti-copy/anti-print measures and company watermarking
- Campaign portal links to Emergent HSE App for incident reporting via email
