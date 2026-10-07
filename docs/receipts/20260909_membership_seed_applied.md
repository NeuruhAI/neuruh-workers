# MEMBERSHIP SEED — MIGRATION APPLIED — RECEIPT

**Registry:** RECIPE-MEMBERSHIP-SEED-20260909 — `BLOCKED-SCHEMA` → **`APPLIED`**
**Milestone:** M8 — membership seed
**Date:** 2026-09-09
**Outcome:** **PASS — table created, 20,738 PRIMARY_PROJECTION rows seeded, ledger reconciled.**

The prior "no authoritative DDL exists anywhere" finding is **withdrawn**. It searched
`recipe-engine`. The DDL is in `deedsonar`, and it is the current production deploy.

---

## 1. THE MIGRATION — LOCATED AND READ

`deedsonar` `supabase/migrations/20260907103000_candidate_recipe_membership_v1.sql`
at `b4ca64c` — *"Recipe P0: normalized candidate_recipe_membership seam (#52)"* —
which is the **current `origin/main` HEAD** of deedsonar. Merged, shipped, and
applied to **neither** database. Third instance of MERGED != WIRED today.

### Complete DDL

**Preflight (fails closed):** aborts unless `public.lead_import_rows`,
`public.candidate_recipe_definitions`, and `public.app_current_tenant()` all exist.
Wrapped in `begin/commit` with `lock_timeout = 5s`, `statement_timeout = 120s`.

**Columns**

| column | type | null | default |
|---|---|---|---|
| `tenant_id` | `uuid` | NOT NULL | — |
| `import_row_id` | `uuid` | NOT NULL | — |
| `recipe_id` | `text` | NOT NULL | — |
| `recipe_version` | `text` | NOT NULL | `''` |
| `scoring_version` | `text` | NOT NULL | `''` |
| `membership_class` | `text` | NOT NULL | — |
| `recipe_rank` | `integer` | NULL | — |
| `recipe_score` | `numeric` | NULL | — |
| `source_receipt_id` | `text` | NOT NULL | `''` |
| `explanation` | `text` | NOT NULL | `''` |
| `observed_at` | `timestamptz` | NULL | — |
| `created_at` | `timestamptz` | NOT NULL | `now()` |
| `updated_at` | `timestamptz` | NOT NULL | `now()` |

Blank `recipe_version` / `scoring_version` are, by the migration's own comment,
**explicit UNKNOWN/legacy projection — not invented values**.

**Constraints**

- PK `(tenant_id, import_row_id, recipe_id, recipe_version, scoring_version)`
- FK `import_row_id` → `lead_import_rows(row_id)` **ON DELETE CASCADE**
- FK `recipe_id` → `candidate_recipe_definitions(recipe_id)` **ON UPDATE CASCADE ON DELETE RESTRICT**
- CHECK `membership_class IN ('PRIMARY_PROJECTION','ELIGIBLE','PROXY','HELD')`
- CHECK `recipe_rank IS NULL OR recipe_rank > 0`

**Indexes**

- `candidate_recipe_membership_recipe_idx (tenant_id, recipe_id, membership_class, recipe_rank)`
- `candidate_recipe_membership_candidate_idx (tenant_id, import_row_id, membership_class)`
- `candidate_recipe_membership_one_primary_idx` — **UNIQUE `(tenant_id, import_row_id)` WHERE `membership_class = 'PRIMARY_PROJECTION'`**

**RLS — the migration DOES create policy. It does not land unprotected.**

```sql
alter table public.candidate_recipe_membership enable row level security;
create policy candidate_recipe_membership_tenant_select
  for select to authenticated
  using (tenant_id = (select public.app_current_tenant()));
revoke all on public.candidate_recipe_membership from public, anon;
grant select on public.candidate_recipe_membership to authenticated;
grant all    on public.candidate_recipe_membership to service_role;
```

**This does not intersect M1 as a defect.** RLS is enabled *and* carries a policy,
`anon` and `public` are revoked, and no write path is granted to `authenticated` —
all writes must come through `service_role`. Post-apply, the Supabase security
advisor does **not** list this table under `rls_enabled_no_policy` (12 other tables
are listed; this is not one of them).

**Post-seed court (fails closed):** raises if any non-empty `primary_recipe` on
`lead_import_rows` fails to resolve to `candidate_recipe_definitions`.

### The migration's own scope boundary, verbatim

> "First apply seeds only the already-existing primary_recipe projection. It
> invents no secondary memberships. Later Recipe Engine receipts may append
> ELIGIBLE/PROXY/HELD memberships through a separately-courted writer path."

Honored. **No secondary memberships were loaded in this lane.**

### Settled question

`membership_class` is a CHECK over exactly four values. Recipe semantics —
`fractional`, `heir`, `tax_foreclosure_curative`, `tf_fractional` — live in
`recipe_id`, FK to `candidate_recipe_definitions`. They **cannot** go in
`membership_class`. Confirmed against the shipped DDL.

---

## 2. APPLIED TO THE PRO TARGET

**Target:** `Deed Sonar Launch` — `efxowqudmsfqjmbeidjc` — org `APG Land`
(`eepkehgpqnvjeuvxcmcq`), plan **pro**, `ACTIVE_HEALTHY`, Postgres 17.6.1.166.

Selected by evidence, not by name: it is the only reachable project holding
`lead_import_rows` + `candidate_recipe_definitions` + `app_current_tenant()`.
`Central Command`, `Land CRM B Test`, and `APG Land CRM` hold none of them.

The Supabase CLI link in `deedsonar/supabase/.temp/project-ref` points at
`lvdimujkfheeyqchjzra`, **which is not in this organization and is not reachable
with the authenticated credential**. The CLI migration path was therefore not
usable. The SQL was applied directly, verbatim, minus only the outer
`begin;`/`commit;` (supplied by the executor).

### Ledger reconciled

`20260907103000` was **absent** from `supabase_migrations.schema_migrations`.
The estate had been rebuilt by a `p3_*` replay on 2026-09-08 (through
`p3_109_county_import_address_key_parity`) that never included this migration.

The version is now recorded:

| version | name |
|---|---|
| `20260907103000` | `candidate_recipe_membership_v1` |

---

## 3. SHAPE VERIFIED

Table exists with the **exact** expected shape. All 13 columns, types, nullability
and defaults as specified. All 5 constraints present as specified. All 4 indexes
`indisvalid = true`, `indisready = true`, `indislive = true`.

**Partial unique index — present and valid:**

```
CREATE UNIQUE INDEX candidate_recipe_membership_one_primary_idx
  ON public.candidate_recipe_membership USING btree (tenant_id, import_row_id)
  WHERE (membership_class = 'PRIMARY_PROJECTION');
```

**Grants observed:** `authenticated` = SELECT only; `service_role` = ALL;
`anon` = **none**. Policy `candidate_recipe_membership_tenant_select` present,
`FOR SELECT`, role `authenticated`, `USING (tenant_id = (SELECT app_current_tenant()))`.

**Known gap (not introduced here, inherent to the shipped DDL):** the table has
`updated_at NOT NULL DEFAULT now()` but **no trigger maintains it** (0 user
triggers). The separately-courted writer path must set `updated_at` explicitly.

---

## 4. SEED — PRIMARY_PROJECTION ONLY

| metric | value |
|---|---:|
| Source rows with non-empty `primary_recipe` | **20,738** |
| Rows seeded | **20,738** |
| `membership_class = 'PRIMARY_PROJECTION'` | **20,738** |
| Any other `membership_class` | **0** |
| Distinct `import_row_id` | **20,738** |
| Rows rejected by the partial unique index | **0** |
| Unresolved recipe definitions (post-seed court) | **0** |
| Tenants | 1 |
| Table size | 31 MB |

One membership per candidate, one candidate per membership. **Zero loss** —
source count equals seeded count. The partial unique index rejected nothing,
which is itself the proof that production held no duplicate primary claim.

### Seeded distribution

| recipe_id | recipe_version | rows | ranked | scored |
|---|---|---:|---:|---:|
| `published_tax_distress` | `nc_curative_recipes.v1` | 18,909 | 18,909 | 18,909 |
| `fractional_family_ownership` | `nc_curative_recipes.v1` | 976 | 976 | 976 |
| `heir_property_acquisition_research` | `nc_curative_recipes.v1` | 599 | 599 | 599 |
| `long_term_absentee_potential_equity` | `nc_curative_recipes.v1` | 148 | 148 | 148 |
| `tax_foreclosure_curative` | `nc_curative_recipes.v1` | 67 | 67 | 67 |
| `stalled_tax_foreclosure_heirship` | `nc_curative_recipes.v1` | 33 | 33 | 33 |
| `estate_sale_life_event_research` | *(blank)* | **4** | 0 | 0 |
| `tax_foreclosure_fractional_title` | `nc_curative_recipes.v1` | 2 | 2 | 2 |
| **total** | | **20,738** | | |

`scoring_version` is `''` for every row — `candidate_recipe_definitions` carries no
scoring version for any of these recipes. Per the DDL comment this is an explicit
UNKNOWN, not an invention.

---

## 5. THE `property_key` → `import_row_id` MAPPING — **PROVEN**

The unresolved mapping is now resolved, by set equality rather than inference.

**Fable's artifact was located** (the brief did not name its path):
`~/neuruh-workspace/RECIPE_MULTIMEMBERSHIP_20260909/private/projection1/DEEDSONAR_MEMBERSHIP_PROJECTION.csv`
`sha256 d5758e9d27394d51dbc8a5573e742c0ab77012e703b6e94323167c5f5a48bcf1` — matching
its own `PROJECTION_RECEIPT.json`. Produced by `fable/multi-membership-emission-20260909`
at `abdd429`. Contract `deedsonar.membership-projection.v1`.

Its content confirms the brief's figures exactly: **21,375 membership rows over
20,734 properties; 20,734 primary; 641 secondary; 564 multi-member properties.**

### Where `property_key` lives in the database

Not a column. It is `lead_import_rows.mapped->>'property_key'`, mirrored identically
at `normalized->>'property_key'` and `recipe_engine->>'property_key'`. Format
`STATE:FIPS:APN`, identical to Fable's — 0 rows off-format.

### The proof

| test | result |
|---|---|
| Rows carrying `mapped->>'property_key'` | 20,734 |
| Distinct values | 20,734 |
| **Ambiguous (one key → many rows)** | **0** |
| `mapped` pk ≠ `normalized` pk | 0 |
| Fable distinct `property_key` | 20,734 |
| **md5 of sorted distinct key set — Fable** | **`aaa202efedf5c45850e6bd155eb25aa4`** |
| **md5 of sorted distinct key set — database** | **`aaa202efedf5c45850e6bd155eb25aa4`** |

**The two key spaces are byte-for-byte identical.** Combined with zero ambiguity,
`property_key` → `lead_import_rows.row_id` is a **bijection**.

### Mapping coverage — the input to the separately-courted writer path

| | count | share |
|---|---:|---:|
| Fable `property_key`s resolving to a `row_id` | **20,734** | **100.00%** |
| Failing to resolve | **0** | 0.00% |

All 20,734 also carry a `primary_recipe`, so **every one is already seeded**.
All four secondary `recipe_id`s (`fractional_family_ownership` 552,
`tax_foreclosure_curative` 35, `heir_property_acquisition_research` 33,
`tax_foreclosure_fractional_title` 21) exist in `candidate_recipe_definitions`,
so the `recipe_id` FK will hold for all 641. `recipe_version` is
`nc_curative_recipes.v1` on all 641, matching the seeded primaries — so the
composite PK will not collide with a seeded row.

**The 641 secondary memberships were NOT loaded.** That remains outside this
migration's authority.

### ⚠️ FINDING FOR THE WRITER PATH — Fable's `is_primary` DISAGREES WITH PRODUCTION ON 148 PROPERTIES

The key sets match; the **primary assignments do not**.

| | md5 of sorted `property_key\|primary recipe_id` |
|---|---|
| Fable `is_primary=true` | `3f53bcfee7973f9f16c10138c4b9ebbd` |
| Database `PRIMARY_PROJECTION` | `4bcea73dd69db7c3f1f4a9ff7020b32b` |

The disagreement set is **exactly 148 properties** — the entire
`long_term_absentee_potential_equity` cohort. For all 148, production asserts
`long_term_absentee_potential_equity`; Fable's re-emission asserts the fallthrough
`published_tax_distress`, and **never emits the absentee recipe at any rank**.

This is not a defect in either artifact. It is Fable's own documented limitation:
`out_of_state` is half of a conjunction and is not a positive ladder predicate, so
the absentee recipe is *inexpressible* from that emission.

**Consequence, and it is load-bearing:** if the writer path loads Fable's
`is_primary=true` rows as `PRIMARY_PROJECTION`, **148 properties would be silently
downgraded** from a real absentee thesis to the fallthrough. The partial unique
index would reject the insert rather than allow a second primary — the database
fails closed — but the writer must not attempt it. **Load the 641 secondaries only,
under `ELIGIBLE`/`PROXY`/`HELD`; leave every seeded `PRIMARY_PROJECTION` untouched.**

---

## 6. THE FOUR-ROW DELTA — **EXPLAINED, NOT UNEXPLAINED**

The brief inverted the two figures. Correcting it plainly:

- **20,738** is what the migration seeds — its `WHERE coalesce(primary_recipe,'') <> ''`
  matches 20,738 rows. This equals the **RC16 P8 expectation**, which is correct.
- **20,734** is Fable's property count, and is *not* the expected primary seed count.

The 4 rows are these, and they are identified individually:

| row_id | row_num | property_key | recipe |
|---|---:|---|---|
| `7b358407-e51f-40ff-ae77-c34130ead03e` | 22340 | `MI:26021:11-12-5220-0128-00-0` | `estate_sale_life_event_research` |
| `724d5a94-cb10-40fa-b1ed-11a4a6d5726f` | 22341 | `MI:26021:11-17-0006-0001-04-0` | `estate_sale_life_event_research` |
| `088b4209-6299-4cf6-8e6c-08a0fca614e5` | 22342 | `MI:26021:11-08-0018-0004-02-7` | `estate_sale_life_event_research` |
| `4ebce89c-6763-4a52-96de-3dda2266cf78` | 22343 | `MI:26021:11-02-0014-0015-03-0` | `estate_sale_life_event_research` |

All four share batch `bcba5e48-cc55-4204-958c-d048de6b94d2`, are the **last four rows
of the estate** by `row_num`, and are `status = staged`,
`candidate_class = RESEARCH_CANDIDATE`, `map_status = GEOCODE_REQUIRED`,
`recipe_engine IS NULL`.

**Why they are not in Fable's 20,734:** they are **Michigan** (`MI:26021`, Berrien
County). Fable's ladder is `nc_curative_recipes.v1` — North Carolina only; its
entire universe is `NC:*`. These four rows were never ranked by the Recipe Engine
(`recipe_engine IS NULL`), so they carry `normalized->>'property_key'` but no
`mapped->>'property_key'` and no NC recipe receipt.

They seeded correctly and honestly: blank `recipe_version`, blank
`source_receipt_id`, NULL `recipe_rank`, NULL `recipe_score`, and the migration's
default explanation — the DDL's "explicit UNKNOWN/legacy projection" convention
doing exactly its job.

**20,738 = 20,734 (NC, Recipe-Engine-ranked) + 4 (MI, unranked).** Nothing is
unexplained and nothing is missing.

---

## 7. WHAT WAS NOT DONE

- **The 641 secondary memberships were not loaded.** Outside this migration's authority.
- **The second database was not updated.** `lvdimujkfheeyqchjzra` — the ref in
  deedsonar's CLI link — is outside the authenticated organization and unreachable
  from here. It remains unapplied. Reaching it needs a credential this session does
  not hold.
- **No change** to `deedsonar_result_set_scope_v1`, QuerySpec, ranking, source
  hunting, callability, CRM state, or production outreach.
- `lead_import_rows.primary_recipe` remains the default projection, untouched.

## 8. REGISTRY

`RECIPE-MEMBERSHIP-SEED-20260909`: `BLOCKED-SCHEMA` → **`APPLIED`**

The block is cleared. The block's premise — that no authoritative DDL existed — was
false; the DDL was in `deedsonar` all along and is now applied, seeded, and verified
on the Pro target.
